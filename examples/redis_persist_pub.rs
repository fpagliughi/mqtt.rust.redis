// redis_persist_pub.rs
//
// Example/test for mqtt-redis.
//
// This shows how to to use mqtt-redis with the Paho MQTT Rust library
// in order to have a local Redis server as the persistence store for the
// messaging application.
//

// --------------------------------------------------------------------------
// Copyright (c) 2017-2023 Frank Pagliughi <fpagliughi@mindspring.com>
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

use std::{env, process};

use paho_mqtt as mqtt;
use paho_mqtt_redis::RedisPersistence;

// --------------------------------------------------------------------------

fn main() {
    // Use the environment logger for this example.
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "tcp://localhost:1883".to_string());

    println!("Connecting to MQTT broker at: '{}'", host);

    // Create a client & define connect options
    let persistence = RedisPersistence::new();

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(host)
        .client_id("rust_redis_pub")
        .user_persistence(persistence)
        .finalize();

    let cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|err| {
        match err {
            mqtt::Error::Paho(-2 /*mqtt::PERSISTENCE_ERROR*/) => {
                eprintln!("Error connecting to the local Redis server. Is it running?")
            }
            _ => eprintln!("Error creating the client: {:?}", err),
        };
        process::exit(2);
    });

    // Connect and wait for it to complete or fail
    if let Err(e) = cli.connect(None).wait() {
        println!("Unable to connect: {:?}", e);
        process::exit(1);
    }

    // Create a message and publish it
    // Use non-zero QoS to exercise message persistence
    println!("Publishing a message to 'test' topic");

    let msg = mqtt::Message::new("test", "Hello world!", mqtt::QOS_1);
    let tok = cli.publish(msg);

    if let Err(e) = tok.wait() {
        println!("Error sending message: {:?}", e);
    }

    // Disconnect from the broker
    println!("Disconnecting from the broker.");

    let tok = cli.disconnect(None);
    tok.wait().unwrap();

    println!("Done");

    drop(cli);
    println!("Exiting");
}
