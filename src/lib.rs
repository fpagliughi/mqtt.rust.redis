// mqtt.rust.redis/src/lib.rs
// 
// Main library source file for 'mqtt-redis'.
//
// --------------------------------------------------------------------------
// Copyright (c) 2017-2020 Frank Pagliughi <fpagliughi@mindspring.com>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived from this
// software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 

//! This is a small example of using Redis as the persistence store for the
//! Paho MQTT Rust client.
//!
//! It is an add-on library for use with the Eclipse Paho Rust MQTT Client
//!     <https://github.com/eclipse/paho.mqtt.rust>
//!
//! The MQTT client library provides several mechanisms to persist QoS 1 & 2
//! messages while they are in transit. This helps to ensure that even if the
//! client application crashes, upon restart those messages can be retrieved
//! from the persistence store and re-sent to the server.
//!
//! The Paho library contains file/disk based persistence out of the box.
//! That is very useful, but on a Flash-based Embedded device, like an IoT
//! gateway, but continuous writes to the flash chip will wear it out
//! prematurely.
//!
//! So it would be nice to use a RAM-based cache that is outside the client
//! application's process. An instance of Redis, running locally, is a
//! nice solution.
//!
//! The Paho library allows the application to create a user-supplied
//! persistence object and register that with the client. The object simply
//! needs to implement the `paho_mqtt::ClientPersistence` trait. These
//! callbacks map to the operations on a key/value store, so Redis is a
//! perfect candidate to match the persistence API and act as a store.
//!
//! The MQTT callbacks map nearly 1:1 to Redis Hash commands:
//!
//! ```ignore
//!      open()      -> conect
//!      close()     -> disconnect
//!
//!      put()       -> HSET
//!      get()       -> HGET
//!      remove()    -> HDEL
//!      keys()      -> HKEYS
//!      clear()     -> DEL
//!      contains_key() -> HEXISTS
//!```
//!
//! NOTE: Using Redis as an MQTT persistence store is an extremely viable
//! solution in a production IoT device or gateway, but it really only makes
//! sense to use it if the Redis server is running locally on the device
//! and connected via localhost or a UNIX socket. It _does not make sense_ to
//! use a remote Redis server for this purpose.
//!

#[macro_use] extern crate log;

use paho_mqtt as mqtt;
use redis::{Client, Commands, Connection, RedisResult };

// --------------------------------------------------------------------------

/// The MQTT Redis persistence object.
/// An instance of this stuct can be residtered with an MQTT client to hold
/// messgaes in a Redis server until they are properly acknowledged by the
/// remote MQTT server. An instance of this object maps to a single hash
/// on a specific Redis server.
pub struct RedisPersistence {
    /// The name of the Redis hash object.
    /// This is formed as a combination of the MQTT server name/address
    /// and the client ID string.
    name: String,
    /// The Redis client
    client: Client,
    /// The connection to the Redis client.
    /// This is opened and closed on instruction from the MQTT client.
    conn: Option<Connection>,
}

impl RedisPersistence {
    /// Create a new persistence object to connect to a local Redis server.
    pub fn new() -> Self { Self::default() }
}

impl Default for RedisPersistence {
    /// Create a new persistence object to connect to the Redis server
    /// on localhost.
    fn default() -> Self {
        Self {
            name: "".to_string(),
            client: Client::open("redis://localhost/").unwrap(),
            conn: None,
        }
    }
}

impl mqtt::ClientPersistence for RedisPersistence
{
    /// Opena the connection to the Redis client.
    fn open(&mut self, client_id: &str, server_uri: &str) -> mqtt::Result<()> {
        self.name = format!("{}:{}", client_id, server_uri);

        match self.client.get_connection() {
            Ok(conn) => {
                trace!("Redis persistence [{}]: open", self.name);
                self.conn = Some(conn);
                Ok(())
            }
            Err(e) => {
                warn!("Redis persistence connect error: {:?}", e);
                return Err(mqtt::PersistenceError)?
            }
        }
    }

    /// Close the connection to the Redis client.
    fn close(&mut self) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: close", self.name);
        if let Some(conn) = self.conn.take() {
            drop(conn);
        }
        trace!("Redis close complete");
        Ok(())
    }

    /// Store a persistent value to Redis.
    /// We get a vector of buffer references for the data to store, which we
    /// can concatenate into a single byte buffer to send to the server.
    fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: put key '{}'", self.name, key);
        let conn = self.conn.as_mut().ok_or(mqtt::PersistenceError)?;
        let buf: Vec<u8> = buffers.concat();
        debug!("Putting key '{}' with {} bytes", key, buf.len());
        redis::cmd("HSET").arg(&self.name).arg(key).arg(buf).execute(conn);
        Ok(())
    }

    /// Get the data buffer for the requested key.
    /// Although the value sent to the server was a collection of buffers,
    /// we can return them as a single, concatenated buffer.
    fn get(&mut self, key: &str) -> mqtt::Result<Vec<u8>> {
        trace!("Client persistence [{}]: get key '{}'", self.name, key);
        let conn = self.conn.as_mut().ok_or(mqtt::PersistenceError)?;
        if let Ok(v) = conn.hget(&self.name, key) as RedisResult<Vec<u8>> {
            debug!("Found key {} with {} bytes", key, v.len());
            Ok(v)
        }
        else {
            Err(mqtt::PersistenceError)
        }
    }

    /// Remove the value with the specified `key` from the store.
    fn remove(&mut self, key: &str) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: remove key '{}'", self.name, key);
        let conn = self.conn.as_mut().ok_or(mqtt::PersistenceError)?;
        if let Ok(res) = conn.hdel(&self.name, key) as RedisResult<usize> {
            if res != 0 {
                debug!("Removed key: {}", key);
            }
            else {
                debug!("Key not found (assuming OK): {}", key);
            }
            // Either way, if key is not in the store we report success.
            return Ok(());
        }
        Err(mqtt::PersistenceError)
    }

    /// Return a collection of all the keys in the store for this client.
    fn keys(&mut self) -> mqtt::Result<Vec<String>> {
        trace!("Client persistence [{}]: keys", self.name);
        let conn = self.conn.as_mut().ok_or(mqtt::PersistenceError)?;
        if let Ok(v) = conn.hkeys(&self.name) as RedisResult<Vec<String>> {
            debug!("Found keys: {:?}", v);
            Ok(v)
        }
        else {
            warn!("Error looking for keys");
            Err(mqtt::PersistenceError)
        }
    }

    /// Remove all the data for this client from the store.
    fn clear(&mut self) -> mqtt::Result<()> {
        trace!("Client persistence [{}]: clear", self.name);
        let conn = self.conn.as_mut().unwrap(); // TODO: Check for error?
        if let Ok(_res) = conn.del(&self.name) as RedisResult<usize> {
            // res==1 means hash/store deleted, 0 means it wasn't found.
            // Either way, it's gone, so return success
            return Ok(());
        }
        Err(mqtt::PersistenceError)
    }

    /// Determines if the store for this client contains the specified `key`.
    fn contains_key(&mut self, key: &str) -> bool {
        trace!("Client persistence [{}]: contains key '{}'", self.name, key);
        let conn = match self.conn.as_mut() {
            Some(conn) => conn,
            None => return false,
        };
        if let Ok(res) = conn.hexists(&self.name, key) as RedisResult<usize> {
            debug!("'contains' query returned: {:?}", res);
            res != 0
        }
        else { false }
    }
}

