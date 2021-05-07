/**
 * Copyright (c) 2014-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fbzmq/async/AsyncSignalHandler.h>
#include <fbzmq/zmq/Common.h>

#ifndef IS_BSD
#include <sys/signalfd.h>
#else
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#endif

#include <unistd.h>

#include <folly/Format.h>

namespace fbzmq {

AsyncSignalHandler::AsyncSignalHandler(ZmqEventLoop* evl) : evl_(evl) {
  CHECK(evl_) << "Error: event loop empty";

  if (sigemptyset(&registeredSignals_) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to empty a signal mask";
  }

#ifndef IS_BSD
  // Create signal-fd for signal handling
  if ((signalFd_ = signalfd(
           -1 /* create a new fd */,
           &registeredSignals_,
           SFD_CLOEXEC /* flags */)) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to create a signalfd.";
  }
#else
  if ((signalFd_ = kqueue()) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to create a signalfd.";
  }
  util::setFdCloExec(signalFd_);
#endif
  // Attach callback on signal fd
  evl_->addSocketFd(signalFd_, ZMQ_POLLIN, [this](int revents) noexcept {
    CHECK(revents & ZMQ_POLLIN);
    int sig;
    // Receive signal
#ifndef IS_BSD
    struct signalfd_siginfo fdsi;
    auto bytesRead = read(signalFd_, &fdsi, sizeof(fdsi));
    CHECK_EQ(sizeof(fdsi), bytesRead);
    sig = static_cast<int>(fdsi.ssi_signo);
#else
    struct kevent kInEv;
    int r = kevent(signalFd_, NULL, 0, &kInEv, 1, NULL);
    if (r < 0 || kInEv.filter != EVFILT_SIGNAL) {
      PLOG(FATAL) << "AsyncSignalHandler: Failed to read signalfd.";
    }
    sig = static_cast<int>(kInEv.ident);
#endif
    VLOG(1) << "AsyncSignalHandler: Received signal " << sig;
    signalReceived(sig);
  });
}

AsyncSignalHandler::~AsyncSignalHandler() {
  close(signalFd_);
}

void
AsyncSignalHandler::setupSignal(int sig, bool isAdding) {
  int ret = sigismember(&registeredSignals_, sig);
  if (ret < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: invalid/unsupported signal number "
                << sig;
  }
  if (ret == (isAdding ? 1 : 0)) {
    // This signal has already been registered
    throw std::runtime_error(
        folly::sformat("handler already registered for signal {}", sig));
  }

  // Block the signal so that it's not handled according to its default
  // disposition
  sigset_t mask;
  if (sigemptyset(&mask) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to empty a signal mask";
  }
  if (sigaddset(&mask, sig) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to add a signal into a mask";
  }
  if (pthread_sigmask(isAdding ? SIG_BLOCK : SIG_UNBLOCK, &mask, nullptr) != 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to block signals";
  }

  // Update signal-fd
  if (isAdding && sigaddset(&registeredSignals_, sig) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to add a signal into a mask";
  } else if (!isAdding && sigdelset(&registeredSignals_, sig) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to delete a signal from a mask";
  }
#ifndef IS_BSD
  if ((signalFd_ = signalfd(
           signalFd_ /* update fd */,
           &registeredSignals_,
           SFD_CLOEXEC /* flags */)) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to update signalfd.";
  }
#else
  struct kevent sigevent;
  if (isAdding) {
    // Kqueue signal events are processes after legacy handlers,
    // Mark then as ignored so kqueue can see it.
    signalHandlers_[sig] = signal(sig, SIG_IGN);
    EV_SET(&sigevent, sig, EVFILT_SIGNAL, EV_ADD | EV_ENABLE, 0, 0, NULL);
  } else {
    EV_SET(&sigevent, sig, EVFILT_SIGNAL, EV_DELETE, 0, 0, NULL);
    signal(sig, signalHandlers_[sig]);
    signalHandlers_[sig] = NULL;
  }
  if (kevent(signalFd_, &sigevent, 1, NULL, 0, NULL) < 0) {
    PLOG(FATAL) << "AsyncSignalHandler: Failed to update signalfd.";
  }
#endif
}

void
AsyncSignalHandler::registerSignalHandler(int sig) {
  setupSignal(sig, true);
}

void
AsyncSignalHandler::unregisterSignalHandler(int sig) {
  setupSignal(sig, false);
}

ZmqEventLoop*
AsyncSignalHandler::getZmqEventLoop() {
  return evl_;
}

} // namespace fbzmq
