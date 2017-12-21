// async_publish.rs
// 
// Example/test for Paho MQTT Rust library.
//

/*******************************************************************************
 * Copyright (c) 2017 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

extern crate log;
extern crate env_logger;

extern crate paho_mqtt as mqtt;
extern crate mqtt_redis;

use mqtt_redis::MemPersistence;

// --------------------------------------------------------------------------

fn main() {
	env_logger::init().unwrap();

	println!("Connecting to MQTT broker.");
	// Create a client & define connect options
	let persistence = MemPersistence::new();

	let create_opts = mqtt::CreateOptionsBuilder::new()
			.server_uri("tcp://localhost:1883")
			.user_persistence(persistence)
			.finalize();

	let cli = mqtt::AsyncClient::with_options(create_opts);
	let conn_opts = mqtt::ConnectOptions::new();

	// Connect and wait for it to complete or fail
	if let Err(e) = cli.connect(conn_opts).wait() {
		println!("Unable to connect:\n\t{:?}", e);
		::std::process::exit(1);
	}

	// Create a message and publish it
	println!("Publishing a message to 'test' topic");
	let msg = mqtt::Message::new("test", "Hello world!");
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
