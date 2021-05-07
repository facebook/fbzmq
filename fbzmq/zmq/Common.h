/**
 * Copyright (c) 2014-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <ostream>
#include <string>

#include <folly/Optional.h>
#include <folly/String.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <sodium.h>
#include <zmq.h>

namespace fbzmq {

/**
 * Generic error wrapper for ZMQ operations
 */
struct Error {
  // capture error from zmq_errno()
  Error();
  explicit Error(int errNum);
  Error(int errNum, std::string errString);

  const int errNum{0};
  const std::string errString;
};

std::ostream& operator<<(std::ostream&, Error const&);

/**
 * Crypto key pair
 */
struct KeyPair {
  std::string privateKey;
  std::string publicKey;
};

/**
 * PollIem ... same as zmq_pollitem_t
 */
using PollItem = zmq_pollitem_t;

/**
 * Polling. poll indefinitely by default
 */
folly::Expected<int, Error> poll(
    std::vector<PollItem> const& items,
    folly::Optional<std::chrono::milliseconds> timeout = folly::none);

/**
 * Proxy connects a frontend socket to a backend socket.
 * Conceptually, data flows from frontend to backend.
 * Depending on the socket types, replies may flow in the opposite direction.
 */
folly::Expected<folly::Unit, Error> proxy(
    void* frontend, void* backend, void* capture);

namespace util {

#ifdef IS_BSD
void setFdCloExec(int fd);
#endif

/**
 * Generate a crypto key pair
 */
KeyPair genKeyPair();

/**
 * Utility functions for conversion between thrift objects and string/IOBuf
 */

template <typename ThriftType, typename Serializer>
std::unique_ptr<folly::IOBuf>
writeThriftObj(ThriftType const& obj, Serializer& serializer) {
  auto queue = folly::IOBufQueue();
  serializer.serialize(obj, &queue);

  return queue.move();
}

template <typename ThriftType, typename Serializer>
std::string
writeThriftObjStr(ThriftType const& obj, Serializer& serializer) {
  std::string result;
  serializer.serialize(obj, &result);
  return result;
}

template <typename ThriftType, typename Serializer>
ThriftType
readThriftObj(folly::IOBuf& buf, Serializer& serializer) {
  ThriftType obj;
  serializer.deserialize(&buf, obj);
  return obj;
}

template <typename ThriftType, typename Serializer>
ThriftType
readThriftObjStr(const std::string& buf, Serializer& serializer) {
  ThriftType obj;
  serializer.deserialize(buf, obj);
  return obj;
}

} // namespace util
} // namespace fbzmq
