/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <fbzmq/async/ZmqEventLoop.h>

#include <sys/eventfd.h>
#include <unistd.h>

#include <folly/Format.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>

#include <fbzmq/zmq/Common.h>

namespace fbzmq {

ZmqEventLoop::ZmqEventLoop(uint64_t queueCapacity)
    : callbackQueue_(queueCapacity) {
  // Create signal-fd for start/stop events
  if ((signalFd_ = eventfd(0 /* init-value */, 0 /* flags */)) < 0) {
    LOG(FATAL) << "ZmqEventLoop: Failed to create an eventfd.";
  }

  // Create signal-fd for callback events from external threads
  if ((callbackFd_ = eventfd(0 /* init-value */, EFD_NONBLOCK)) < 0) {
    LOG(FATAL) << "ZmqEventLoop: Failed to create an eventfd.";
  }

  // Attach callback on signal fd
  addSocketFd(signalFd_, ZMQ_POLLIN, [this](int revents) noexcept {
    CHECK(revents & ZMQ_POLLIN);

    // Receive 8 byte integer
    uint64_t buf;
    auto bytesRead = read(signalFd_, static_cast<void*>(&buf), sizeof(buf));
    CHECK_EQ(sizeof(buf), bytesRead);

    VLOG(4) << "ZmqEventLoop: Received stop signal. Stopping thread.";
    stop_ = true;
  });

  // Attach callback on callback event fd
  addSocketFd(callbackFd_, ZMQ_POLLIN, [this](int revents) noexcept {
    CHECK(revents & ZMQ_POLLIN);

    // Receive 8 byte integer
    uint64_t buf;
    auto bytesRead = read(callbackFd_, static_cast<void*>(&buf), sizeof(buf));
    CHECK_EQ(sizeof(buf), bytesRead);

    // Process events
    VLOG(4) << "ZmqEventLoop: Received callback events in queue. #" << buf;
    TimeoutCallback callback;
    auto items = callbackQueue_.size();
    VLOG(4) << "ZmqEventLoop: Processing " << items << " callback from queue.";
    while (items-- > 0) {
      callbackQueue_.blockingRead(callback);
      callback();
    }
  });
}

ZmqEventLoop::~ZmqEventLoop() {
  close(signalFd_);
  close(callbackFd_);
}

void
ZmqEventLoop::run() {
  // Must not be running when started/resumed
  CHECK(!isRunning()) << "Calling run() on already running thread";

  // Store the current threadId. All API calls must happen within thread
  threadId_.store(pthread_self(), std::memory_order_relaxed);
  SCOPE_EXIT {
    threadId_.store({}, std::memory_order_relaxed);
  };

  // Start the magic
  loopForever();
}

void
ZmqEventLoop::stop() {
  CHECK(isRunning()) << "Attempt to stop a non-running thread";

  // Send signal on the signalFd_ (eventfd)
  uint64_t buf{1};
  auto bytesWritten = write(signalFd_, static_cast<void*>(&buf), sizeof(buf));
  CHECK_EQ(sizeof(buf), bytesWritten);
}

void
ZmqEventLoop::addSocket(
    RawZmqSocketPtr socketPtr, int events, SocketCallback callback) {
  CHECK(isInEventLoop());
  CHECK_NE(0, events) << "Subscription events can't be empty.";
  if (socketMap_.count(socketPtr)) {
    throw std::runtime_error("Socket callback already registered.");
  }

  auto subscription =
      std::make_shared<PollSubscription>(events, std::move(callback));
  socketMap_.emplace(socketPtr, std::move(subscription));
  needsRebuild_ = true;
}

void
ZmqEventLoop::addSocketFd(int socketFd, int events, SocketCallback callback) {
  CHECK(isInEventLoop());
  CHECK_NE(0, events) << "Subscription events can't be empty.";
  if (socketFdMap_.count(socketFd)) {
    throw std::runtime_error("Socket callback already registered.");
  }

  auto subscription =
      std::make_shared<PollSubscription>(events, std::move(callback));
  socketFdMap_.emplace(socketFd, std::move(subscription));
  needsRebuild_ = true;
}

void
ZmqEventLoop::removeSocket(RawZmqSocketPtr socketPtr) {
  CHECK(isInEventLoop());
  if (socketMap_.erase(socketPtr)) {
    needsRebuild_ = true;
  }
}

void
ZmqEventLoop::removeSocketFd(int socketFd) {
  CHECK(isInEventLoop());
  if (socketFdMap_.erase(socketFd)) {
    needsRebuild_ = true;
  }
}

int64_t
ZmqEventLoop::scheduleTimeout(
    std::chrono::milliseconds timeout, TimeoutCallback callback) {
  CHECK(isInEventLoop());
  return scheduleTimeoutAt(
      std::chrono::steady_clock::now() + timeout, std::move(callback));
}

int64_t
ZmqEventLoop::scheduleTimeoutAt(
    std::chrono::steady_clock::time_point scheduleTime,
    TimeoutCallback callback) {
  CHECK(isInEventLoop());
  auto timeoutId = timeoutId_++;
  timeoutHeap_.emplace(scheduleTime, std::move(callback), timeoutId);
  activeTimeouts_.insert(timeoutId);
  return timeoutId;
}

bool
ZmqEventLoop::cancelTimeout(int64_t timeoutId) {
  CHECK(isInEventLoop());
  return activeTimeouts_.erase(timeoutId) > 0;
}

void
ZmqEventLoop::runInEventLoop(TimeoutCallback callback) {
  // This should never be called from within the thread as it can potentially
  // block itself if queue size if full.
  //
  // Use scheduleTimeout if you need to call from within the thread.
  CHECK(!isRunning() || !isInEventLoop());

  // Enqueue the callback
  callbackQueue_.blockingWrite(std::move(callback));

  // Send signal on the callbackFd_ (eventfd)
  uint64_t buf{1};
  auto bytesWritten = write(callbackFd_, static_cast<void*>(&buf), sizeof(buf));
  CHECK_EQ(sizeof(buf), bytesWritten);
}

void
ZmqEventLoop::runImmediatelyOrInEventLoop(TimeoutCallback callback) {
  if (isInEventLoop()) {
    callback();
    return;
  }

  runInEventLoop(std::move(callback));
}

void
ZmqEventLoop::loopForever() {
  std::chrono::milliseconds pollTimeout;
  stop_ = false;
  while (not stop_) {
    // Rebuild poll-items if needed
    if (needsRebuild_) {
      rebuildPollItems();
      needsRebuild_ = false;
    }

    // Calculate poll-timeout. If there is a pending timeout then poll-timeout
    // will be the amount of duration for that timeout to become active. This
    // is our best try at scheduling request as soon as possible once it becomes
    // active.
    if (not timeoutHeap_.empty()) {
      // Calculate waitTime for next scheduled event
      auto waitTime = std::chrono::duration_cast<std::chrono::milliseconds>(
          timeoutHeap_.top().scheduledTime - std::chrono::steady_clock::now());

      // wait time can be negative if scheduled-timeout is already active
      pollTimeout = std::max(std::chrono::milliseconds(1), waitTime);
    } else {
      // No pending timeouts. Use infinite polling
      pollTimeout = std::chrono::milliseconds(-1);
    }

    // Perform polling on sockets
    VLOG(5) << "ZmqEventLoop: Polling with poll timeout of "
            << pollTimeout.count() << "ms.";
    // this will throw on error
    int count = fbzmq::poll(pollItems_, pollTimeout).value();
    for (size_t i = 0; i < pollItems_.size() && count > 0; ++i) {
      auto& item = pollItems_[i];
      auto& subscription = pollSubscriptions_[i];
      if (item.revents & subscription->events) {
        subscription->callback(item.revents & subscription->events);
        --count;
      }
    } // end for

    // Process timeout heap
    auto now = std::chrono::steady_clock::now();
    while (!timeoutHeap_.empty() && (timeoutHeap_.top().scheduledTime < now)) {
      // Skip processing if timeout is not active
      if (not activeTimeouts_.erase(timeoutHeap_.top().timeoutId)) {
        timeoutHeap_.pop();
        continue;
      }

      // Callback must be issued after popping up the timeout as it can in turn
      // push some more callbacks which can potentially make the callback
      // to be destroyed.
      auto callbackPtr = std::move(timeoutHeap_.top().callbackPtr); // Move ptr
      timeoutHeap_.pop();
      (*callbackPtr)();
    }
  } // end while
}

void
ZmqEventLoop::rebuildPollItems() {
  pollItems_.clear();
  pollSubscriptions_.clear();
  pollItems_.reserve(socketMap_.size() + socketFdMap_.size());
  pollSubscriptions_.reserve(socketMap_.size() + socketFdMap_.size());

  for (auto& kv : socketMap_) {
    pollItems_.push_back(
        {reinterpret_cast<void*>(kv.first), 0, kv.second->events, 0});
    pollSubscriptions_.push_back(kv.second);
  }

  for (auto& kv : socketFdMap_) {
    pollItems_.push_back({nullptr, kv.first, kv.second->events, 0});
    pollSubscriptions_.push_back(kv.second);
  }
}

} // namespace fbzmq
