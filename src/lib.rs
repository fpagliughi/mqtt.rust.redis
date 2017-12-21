// lib.rs
//

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate redis;

extern crate paho_mqtt as mqtt;

use redis::{Client, Commands, Connection, RedisResult, RedisError};

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
		redis::cmd("HSET").arg(&self.name).arg(key).arg(buf).execute(conn);
		Ok(())
	}

	fn get(&self, key: &str) -> mqtt::MqttResult<&[u8]> {
		trace!("Client persistence [{}]: get key '{}'", self.name, key);
		/*
		match self.map.get(key) {
			Some(v) => Ok(&v),
			None => Err(mqtt::PERSISTENCE_ERROR)
		}
		*/
		Err(mqtt::PERSISTENCE_ERROR)
	}

	fn remove(&mut self, key: &str) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: remove key '{}'", self.name, key);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(res) = conn.hdel(&self.name, key) as RedisResult<usize> {
			if res != 0 {
				return Ok(());
			}
		}
		Err(mqtt::PERSISTENCE_ERROR)
	}

	fn keys(&self) -> mqtt::MqttResult<Vec<&str>> {
		trace!("Client persistence [{}]: keys", self.name);
		let mut kv: Vec<&str> = Vec::new();
		/*
		for key in self.map.keys() {
			kv.push(key);
		}
		*/
		Ok(kv)
	}

	fn clear(&mut self) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: clear", self.name);
		let conn = self.conn.as_ref().unwrap();	// TODO: Check for error?
		if let Ok(res) = conn.del(&self.name) as RedisResult<usize> {
			if res != 0 {
				return Ok(());
			}
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
