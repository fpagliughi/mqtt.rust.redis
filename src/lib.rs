// lib.rs
//

#[macro_use]
extern crate log;
extern crate env_logger;

extern crate paho_mqtt as mqtt;

use std::collections::HashMap;

// --------------------------------------------------------------------------

// The ClientPersistence maps pretty closely to a key/val store. We can use
// a Rust HashMap to implement an in-memory persistence pretty easily.

pub struct MemPersistence {
	name: String,
	map: HashMap<String, Vec<u8>>,
}

impl MemPersistence {
	pub fn new() -> MemPersistence {
		MemPersistence {
			name: "".to_string(),
			map: HashMap::new(),
		}
	}
}

impl mqtt::ClientPersistence for MemPersistence
{
	fn open(&mut self, client_id: &str, server_uri: &str) -> mqtt::MqttResult<()> {
		self.name = format!("{}-{}", client_id, server_uri);
		trace!("Client persistence [{}]: open", self.name);
		Ok(())
	}

	fn close(&mut self) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: close", self.name);
		Ok(())
	}

	// We get a vector of buffer references for the data to store, which we 
	// can concatenate into a single byte buffer to place in the map.
	fn put(&mut self, key: &str, buffers: Vec<&[u8]>) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: put key '{}'", self.name, key);
		let buf: Vec<u8> = buffers.concat();
		self.map.insert(key.to_string(), buf);
		Ok(())
	}

	fn get(&self, key: &str) -> mqtt::MqttResult<&[u8]> {
		trace!("Client persistence [{}]: get key '{}'", self.name, key);
		match self.map.get(key) {
			Some(v) => Ok(&v),
			None => Err(mqtt::PERSISTENCE_ERROR)
		}
	}

	fn remove(&mut self, key: &str) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: remove key '{}'", self.name, key);
		match self.map.remove(key) {
			Some(_) => Ok(()),
			None => Err(mqtt::PERSISTENCE_ERROR)
		}
	}

	fn keys(&self) -> mqtt::MqttResult<Vec<&str>> {
		trace!("Client persistence [{}]: keys", self.name);
		let mut kv: Vec<&str> = Vec::new();
		for key in self.map.keys() {
			kv.push(key);
		}
		Ok(kv)
	}

	fn clear(&mut self) -> mqtt::MqttResult<()> {
		trace!("Client persistence [{}]: clear", self.name);
		self.map.clear();
		Ok(())
	}

	fn contains_key(&self, key: &str) -> bool {
		trace!("Client persistence [{}]: contains key '{}'", self.name, key);
		self.map.contains_key(key)
	}
}

/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
