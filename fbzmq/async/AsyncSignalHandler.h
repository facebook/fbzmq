/**
 * Copyright (c) 2014-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <csignal>

#include <fbzmq/async/ZmqEventLoop.h>

#ifdef IS_BSD
#ifdef __APPLE__
#define SIG_MAX __DARWIN_NSIG
#else
#define SIG_MAX NSIG
#endif

#endif

namespace fbzmq {

/**
 * A handler to receive notification about POSIX signals.
 *
 * AsyncSignalHandler allows code to process signals from within a
 * ZmqEventLoop.
 * Standard signal handlers interrupt execution of the main thread, and
 * are run while the main thread is paused.  As a result, great care must be
 * taken to avoid race conditions if the signal handler has to access or modify
 * any data used by the main thread.
 *
 * AsyncSignalHandler solves this problem by running the AsyncSignalHandler
 * callback in normal thread of execution, as a ZmqEventLoop callback.
 *
 * AsyncSignalHandler may only be used in a single thread.  It will only
 * process signals received by the thread where the AsyncSignalHandler is
 * registered.  It is the user's responsibility to ensure that signals are
 * delivered to the desired thread in multi-threaded programs.
 */
class AsyncSignalHandler {
 public:
  /**
   * Create a new AsyncSignalHandler.
   */
  explicit AsyncSignalHandler(ZmqEventLoop* evl);
  virtual ~AsyncSignalHandler();

  /**
   * Register to receive callbacks about the specified signal.
   *
   * Once the handler has been registered for a particular signal,
   * signalReceived() will be called each time this thread receives this
   * signal.
   *
   * Throws an exception if an error occurs, or if this handler is already
   * registered for this signal.
   *
   * NOTE: MUST be called before starting any other thread,
   * otherwise main thread may miss some signal. Maybe becaues other threads
   * created by main() will inherit a copy of the signal mask according to
   * pthread_mask(3)
   */
  void registerSignalHandler(int sig);

  /**
   * Unregister for callbacks about the specified signal.
   *
   * Throws an exception if an error occurs, or if this signal was not
   * registered.
   */
  void unregisterSignalHandler(int sig);

  /**
   * signalReceived() will called to indicate that the specified signal has
   * been received.
   *
   * signalReceived() will always be invoked from the event loop (i.e.,
   * after the main POSIX signal handler has returned control to the event loop
   * thread).
   */
  virtual void signalReceived(int sig) noexcept = 0;

  /**
   * get event loop
   */
  ZmqEventLoop* getZmqEventLoop();

 private:
  // Forbidden copy constructor and assignment operator
  AsyncSignalHandler(AsyncSignalHandler const&);
  AsyncSignalHandler& operator=(AsyncSignalHandler const&);

  void setupSignal(int sig, bool isAdding);

  // event loop
  ZmqEventLoop* evl_{nullptr};

  // Local file descriptor for accepting signals
  int signalFd_{-1};

  // registered signals
  sigset_t registeredSignals_;

#ifdef IS_BSD
  sig_t signalHandlers_[SIG_MAX] {};
#endif
};

} // namespace fbzmq
