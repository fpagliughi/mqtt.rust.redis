// redis_persist_pub.rs
// 
// Example/test for mqtt-redis.
// 
// This shows how to to use mqtt-redis with the Paho MQTT Rust library
// in order to have Redis serve ss the persistence store for the messaging
// application.
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

extern crate log;
extern crate env_logger;

extern crate paho_mqtt as mqtt;
extern crate paho_mqtt_redis;

use std::process;
use paho_mqtt_redis::RedisPersistence;

// Use non-zero QoS to exercise message persistence
const QOS: i32 = 1;

// --------------------------------------------------------------------------

fn main() {
	env_logger::init().unwrap();

	println!("Creating the MQTT client");
	// Create a client & define connect options
	let persistence = RedisPersistence::new();

	let create_opts = mqtt::CreateOptionsBuilder::new()
			.server_uri("tcp://localhost:1883")
			.user_persistence(persistence)
			.finalize();

	let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
		println!("Error creating the client: {:?}", e);
		process::exit(1);
	});

	// Connect and wait for it to complete or fail
	println!("Connecting to MQTT broker.");

	if let Err(e) = cli.connect(None).wait() {
		println!("Unable to connect: {:?}", e);
		process::exit(1);
	}

	// Create a message and publish it
	println!("Publishing a message to 'test' topic");
	let msg = mqtt::Message::new("test", "Hello world!", QOS);
	let tok = cli.publish(msg);

	if let Err(e) = tok.wait() {
		println!("Error sending message: {:?}", e);
	}

	// Disconnect from the broker
	println!("Disconnecting from the broker.");

	let tok = cli.disconnect(None);
	tok.wait().unwrap();

	println!("Done");
}
