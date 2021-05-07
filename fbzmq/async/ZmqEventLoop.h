/**
 * Copyright (c) 2014-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <boost/heap/priority_queue.hpp>
#include <boost/serialization/strong_typedef.hpp>
#include <folly/Function.h>
#include <folly/MPMCQueue.h>
#include <folly/executors/ScheduledExecutor.h>
#include <glog/logging.h>
#include <zmq.h>

#include <fbzmq/async/Runnable.h>

namespace fbzmq {

using SocketCallback = folly::Function<void(int revents) noexcept>;
using TimeoutCallback = folly::Function<void(void)>;

BOOST_STRONG_TYPEDEF(uintptr_t, RawZmqSocketPtr)

/**
 * In ZMQ world thread is all about multiplexing read/write of messages on
 * multiple sockets into a single loop. This class wraps up many basic
 * functionality into one package and makes downward logic more simpler.
 *
 * At a high level this class provides
 * 1. Ability to start and stop thread (thread safe methods)
 * 2. Attach event callback on socket or event fds (dynamic polling list)
 * 3. Schedule async timeouts (must be done from within the thread)
 * 4. Schedule callbacks to run into event loop from other threads
 *
 * Callbacks must be scheduled/registered from within thread itself if loop
 * is running. This has been enforced on all non-thread safe APIs.
 *
 * All API calls except `runInEventLoop` for scheduling timeouts/socket
 * callbacks are not thread safe. If main loop is not running then
 * timeouts/callbacks can be scheduled, assuming they are being done in thread
 * safe manner. The main loop can be stopped and later resumed in some other
 * thread.
 *
 * `runInEventLoop` method allows you to enqueue callbacks into event loop
 * from external threads. The callback will be executed sometime later after
 * the function gets executed into event loop's thread. With `folly::Futures`
 * this API can be used to interact with thread in lock free manner.
 *
 * Usage: You can either inherit this class or create an object and use it as
 * a looper (just like folly::EventBase). Look into `examples` to get started.
 *
 */
class ZmqEventLoop : public virtual Runnable, public folly::ScheduledExecutor {
 public:
  /**
   * ZmqEventLoop constructor
   *
   * We want to make sure to go through event loop at least
   * every healthCheckDuration in worst case to avoid infinite polling or
   * long timeout which casuses unnecessary crash if there's health check
   * mechanism monitoring on latestActivityTs_.
   */
  explicit ZmqEventLoop(
      uint64_t queueCapacity = 1e2,
      std::chrono::seconds healthCheckDuration = std::chrono::seconds(30));

  ~ZmqEventLoop() override;

  /**
   * Make this non-copyable and non-movable
   */
  ZmqEventLoop(ZmqEventLoop const&) = delete;
  ZmqEventLoop& operator=(ZmqEventLoop const&) = delete;

  /**
   * Start a thread. This function will block and return only after stop() has
   * been invoked from any thread.
   *
   * This could be invoked multiple times, e.g. after execution has been stopped
   * i.e. one can do:
   *   run(), waitUntilRunning(), stop(), waitUntilStopped()
   * and then repeat the run/stop cycle again.
   *
   * NOTE: You must invoke repetitive runs from the same thread, otherwise
   *       memory consistency could not be guaranteed.
   */
  void run() override;

  /**
   * Stop sends a request on special eventfd to running thread. On receipt of
   * the signal main-thread will break the loop.
   *
   * This could be invoked from any thread to abort the mainLoop runs. The
   * thread where run has aborted, could then restart looping again, by
   * calling run() repeatedly.
   */
  void stop() override;

  bool
  isRunning() const override {
    return threadId_.load(std::memory_order_relaxed) != 0;
  }

  /**
   * Busy spin until the zmq thread is running. this is called from external
   * threads to synchronize with this thread's main loop
   */
  void
  waitUntilRunning() override {
    while (!isRunning()) {
      std::this_thread::yield();
    }
  }

  /**
   * Similarly, busy spin until this thread has stopped the main loop
   */
  void
  waitUntilStopped() override {
    while (isRunning()) {
      std::this_thread::yield();
    }
  }

  /**
   * Is this thread same as the one in which ZmqEventLoop is being looped. If
   * main loop is not running then this will return true.
   */
  bool
  isInEventLoop() const {
    auto tid = threadId_.load(std::memory_order_relaxed);
    return tid == 0 || pthread_equal(tid, pthread_self());
  }

  /**
   * Add socket/fd to polling list for incoming messages with the callback. Use
   * `events` bitmap for specifying polling events you are interested in.
   *
   * Callback will be invoked with the appropriate revents bitmap.
   */
  void addSocketFd(int socketFd, int events, SocketCallback callback);
  void addSocket(
      RawZmqSocketPtr socketPtr, int events, SocketCallback callback);

  /**
   * Remove socket/fd from polling list. You must remove first before inserting
   * socket/fd for polling with new requested events.
   */
  void removeSocket(RawZmqSocketPtr socketPtr);
  void removeSocketFd(int socketFd);

  /**
   * You can use this function to schedule an async-callback with specified
   * timeout. Callback will be invoked at least after specified duration but
   * no guarantee on upper-time.
   *
   * @returns: A unique token associated with the timeout which you can use
   *           to cancel the timeout if it is still pending.
   */
  int64_t scheduleTimeout(
      std::chrono::milliseconds timeout, TimeoutCallback callback);

  /**
   * Same as above method but takes a time-point after which to call this
   * callback. If two callbacks are inserted with same timestamp then their
   * order of execution will be in the same order as their order of insertion.
   *
   * @returns: A unique token associated with the timeout which you can use
   *           to cancel the timeout if it is still pending.
   */
  int64_t scheduleTimeoutAt(
      std::chrono::steady_clock::time_point scheduleTime,
      TimeoutCallback callback);

  /**
   * Cancel previously scheduled timeout if it exists.
   *
   * NOTE: As of current implementation the scheduled timeout will incur the
   *       memory overhead until its timeout is expired. We keep it in heap
   *       and discard when it is processed.
   *
   * @returns: true if timeout is pending and has been cancelled else returns
   *           false if timeout already got executed.
   */
  bool cancelTimeout(int64_t timeoutId);

  /**
   * ScheduledExecutor & Executor interface
   */
  void
  add(TimeoutCallback callback) override {
    runImmediatelyOrInEventLoop(std::move(callback));
  }
  void
  scheduleAt(
      TimeoutCallback&& callback,
      std::chrono::steady_clock::time_point const& ts) override {
    scheduleTimeoutAt(ts, std::move(callback));
  }

  /**
   * Special method to enqueue function calls into ZmqEventLoop's thread.
   * Function will return immediately and callback will be executed later on.
   *
   * All callbaks enqueued from a single threads are guaranteed to be executed
   * in the same order.
   *
   * NOTE: It can block if underlying queue capacity is full. You can configure
   * it to a bigger number via constructor.
   *
   * NOTE: You can't call this from within the event loop. Must be called from
   * external threads.
   */
  void runInEventLoop(TimeoutCallback callback);

  /**
   * Same as above but can be called from within the loop as well. It will be
   * executed immediately if called from within the same thread otherwise
   * scheduled for later.
   */
  void runImmediatelyOrInEventLoop(TimeoutCallback callback);

  /**
   * Returns the count of currently active timeouts which will get executed
   * eventually.
   */
  size_t
  getNumPendingTimeouts() const {
    return activeTimeouts_.size();
  }

  /**
   * Returns latest activity timestamp as a steady::clock timepoint.
   * This helps in exposing health condition of current thread to monitoring
   * mechanism.
   */
  std::chrono::steady_clock::time_point
  getTimestamp() const noexcept {
    return std::chrono::steady_clock::time_point(
        std::chrono::steady_clock::duration(latestActivityTs_.load()));
  }

  /**
   * return zmq event callback queue size
   */
  size_t
  getEventQueueSize() const {
    return callbackQueue_.allocatedCapacity();
  }

 private:
  /**
   * Utility struct to store information about poll-item and its callback
   * for all added sockets/fds
   */
  struct PollSubscription {
    PollSubscription(int events, SocketCallback&& callback)
        : events(events), callback(std::move(callback)) {}

    // bitmap of subscribed events
    short events{0};

    // callback which needs to be invoked on event.
    SocketCallback callback{nullptr};
  };

  /**
   * Struct to maintain the timeout-state which includes its scheduleTime and
   * associated callback.
   */
  struct TimeoutEvent {
    TimeoutEvent(
        std::chrono::steady_clock::time_point scheduledTime,
        TimeoutCallback&& callback,
        int64_t timeoutId)
        : scheduledTime(scheduledTime), timeoutId(timeoutId) {
      callbackPtr = std::make_shared<TimeoutCallback>(std::move(callback));
    }

    // Define '>' operator for building min-heap
    bool
    operator>(TimeoutEvent const& rhs) const {
      return scheduledTime > rhs.scheduledTime;
    }

    // Time point when this timeout should be executed
    std::chrono::steady_clock::time_point scheduledTime;

    // So that copy will not involve copy of callback
    std::shared_ptr<TimeoutCallback> callbackPtr{nullptr};

    // Unique-ID associated with this timeout
    int64_t timeoutId{0};
  };

  /**
   * All logic for socket polling/timeout invoking happens here.
   */
  void loopForever();

  /**
   * Helper function to rebuild the pollItems_ and pollSubscriptions_ list. This
   * is invoked only after processing of poll items is finished.
   */
  void rebuildPollItems();

#ifndef IS_BSD
  // Local eventfd for capturing stop signal
  int signalFd_{-1};
  // Local eventfd for capturing the signal from newly enqueued callbacks
  // from other threads
  int callbackFd_{-1};
#else
  // Local eventfd for capturing the signal from newly enqueued callbacks
  // from other threads
  int callbackFds_[2]{-1, -1};
  // Local eventfd for capturing stop signal
  int signalFds_[2]{-1, -1};

  void setNonBlockingFd(int fd);
  void createPipeBsd(int fds[]);
#endif

  // Queue to hold externally enqueued events.
  folly::MPMCQueue<TimeoutCallback, std::atomic, true> callbackQueue_;

  // thread-id associated with `run` loop (default value is 0). `threadId_` is
  // also being used to indicate whether a main loop is running or not
  std::atomic<pthread_t> threadId_{};

  // Flag to break the main loop
  bool stop_{false};

  // Data structure to store socket/fd subscriptions along with their callbacks
  std::unordered_map<
      std::uintptr_t /* socket-ptr */,
      std::shared_ptr<PollSubscription>>
      socketMap_{};
  std::unordered_map<int /* socket-fd */, std::shared_ptr<PollSubscription>>
      socketFdMap_{};

  // Polling item. Updated dynamically.
  bool needsRebuild_{false};
  std::vector<zmq_pollitem_t> pollItems_{};
  std::vector<std::shared_ptr<PollSubscription>> pollSubscriptions_{};

  // Priority queue DS for maintaining timeout-heap and callbacks
  boost::heap::priority_queue<
      TimeoutEvent,
      boost::heap::compare<std::greater<TimeoutEvent>>,
      boost::heap::stable<true>>
      timeoutHeap_;

  // Maintains set of active timeouts.
  std::unordered_set<int64_t> activeTimeouts_;

  // Monotonically increasing integer used to assign IDs to newly scheduled
  // timeouts
  int64_t timeoutId_{0};

  // Timestamp of latest callback gets invoked
  std::atomic<std::chrono::steady_clock::duration::rep> latestActivityTs_;

  // Health check duration
  const std::chrono::milliseconds healthCheckDuration_;
};

} // namespace fbzmq
