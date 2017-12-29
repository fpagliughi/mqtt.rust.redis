// lib.rs
// 
// Main library source file for 'mqtt-redis'.
//
// This is a small example of using Redis as the persistence store for the
// Paho MQTT Rust client. The library allows any object to act as a 
// persistence store for messages and other data. The object just needs
// to implement the 'ClientPersistence' trait to service callbacks from 
// the library. These callbacks map to the operations on a key/value 
// store, so Redis is a perfect candidate to act as a store.
//
// --------------------------------------------------------------------------
// Copyright (c) 2017 Frank Pagliughi
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

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate redis;

extern crate paho_mqtt as mqtt;

use redis::{Client, Commands, Connection, RedisResult /*, RedisError*/};

// --------------------------------------------------------------------------

// The ClientPersistence maps pretty closely to a key/val store. We can use
// a Rust HashMap to implement an in-memory persistence pretty easily.

pub struct RedisPersistence {
	name: String,
	client: Client,
	conn: Option<Connection>,
}

impl RedisPersistence {
	pub fn new() -> RedisPersistence {
		RedisPersistence {
			name: "".to_string(),
			client: Client::open("redis://localhost/").unwrap(),
			conn: None,
		}
	}
}

impl mqtt::ClientPersistence for RedisPersistence
{
	fn open(&mut self, client_id: &str, server_uri: &str) -> mqtt::MqttResult<()> {
		self.name = format!("{}:{}", client_id, server_uri);

		match self.client.get_connection() {
			Ok(conn) => {
				trace!("Redis persistence [{}]: open", self.name);
				self.conn = Some(conn);
				Ok(())
			}
			Err(e) => {
				warn!("Redis persistence connect error: {:?}", e);
				return Err(mqtt::PERSISTENCE_ERROR)
			}
		}
	}

	fn close(&mut self) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: close", self.name);
		self.conn = None;
		Ok(())
	}

	// We get a vector of buffer references for the data to store, which we 
	// can concatenate into a single byte buffer to place in the map.
	fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: put key '{}'", self.name, key);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		let buf: Vec<u8> = buffers.concat();
		debug!("Putting key '{}' with {} bytes", key, buf.len());
		redis::cmd("HSET").arg(&self.name).arg(key).arg(buf).execute(conn);
		Ok(())
	}

	// Get the data buffer for the requested key.
	fn get(&self, key: &str) -> mqtt::MqttResult<Vec<u8>> {
		trace!("Client persistence [{}]: get key '{}'", self.name, key);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(v) = conn.hget(&self.name, key) as RedisResult<Vec<u8>> {
			debug!("Found key {} with {} bytes", key, v.len());
			Ok(v)
		}
		else {
			Err(mqtt::PERSISTENCE_ERROR)
		}
	}

	fn remove(&mut self, key: &str) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: remove key '{}'", self.name, key);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
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
		Err(mqtt::PERSISTENCE_ERROR)
	}

	fn keys(&self) -> mqtt::MqttResult<Vec<String>> {
		trace!("Client persistence [{}]: keys", self.name);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(v) = conn.hkeys(&self.name) as RedisResult<Vec<String>> {
			debug!("Found keys: {:?}", v);
			Ok(v)
		}
		else {
			warn!("Error looking for keys");
			Err(mqtt::PERSISTENCE_ERROR)
		}
	}

	fn clear(&mut self) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: clear", self.name);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(_res) = conn.del(&self.name) as RedisResult<usize> {
			// res==1 means hash/store deleted, 0 means it wasn't found.
			// Either way, it's gone, so return success
			return Ok(());
		}
		Err(mqtt::PERSISTENCE_ERROR)
	}

	fn contains_key(&self, key: &str) -> bool {
		trace!("Client persistence [{}]: contains key '{}'", self.name, key);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(res) = conn.hexists(&self.name, key) as RedisResult<usize> {
			debug!("'contains' query returned: {:?}", res);
			res != 0
		}
		else { false }
	}
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
