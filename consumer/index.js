// Copyright 2019-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * This sample demonstrates how to perform basic operations on topics with
 * the Google Cloud Pub/Sub API.
 *
 * For more information, see the README.md under /pubsub and the documentation
 * at https://cloud.google.com/pubsub/docs.
 */

'use strict';


function doHttpRequest(options) {
   const http = require('http');
   return new Promise((resolve, reject) => {
    let data = '';
    const req = http.request(options, (res) => {
        res.on('data', (chunk) => {
            data += chunk;
        });

        res.on('end', () => {
            resolve(data);
        });
    });

    req.on("error", (err) => {
        console.log("Error: " + err.message);
        reject(err);
    });

    req.end();
  });
}

async function getRegion() {
    var options = {
        host: '169.254.169.254',
        path: '/computeMetadata/v1/instance/zone',
        method: 'GET',
        headers: {'Metadata-Flavor': 'Google'},
    };

    var zoneStr = await doHttpRequest(options);

    // zone will be a path like projects/<projectNumber>/<zone>
    // chop off the zone suffix for the region
    var zone = zoneStr.split('/').pop();
    var region = zone.replace(/-[abcdef]$/, '');
    
    return region;


}



// sample-metadata:
//   title: Publish Ordered Message
//   description: Demonstrates how to publish messages to a topic
//     with ordering. Please see "Create Subscription With Ordering" for
//     information on setting up a subscription that will receive the
//     messages with proper ordering.
//   usage: node publishOrderedMessage.js <topic-name> <data>

async function main() {

    var region = await getRegion();
    console.log(region);
  // [START pubsub_publish_with_ordering_keys]
  /**
   * TODO(developer): Uncomment these variables before running the sample.
   */
   var topicName = 'ordered-topic';
  // const data = JSON.stringify({foo: 'bar'});
   var orderingKey = 'key1';

   if (process.env.TOPIC) {
     topicName = process.env.TOPIC;
   }

   if (process.env.ORDERING_KEY) {
     orderingKey = process.env.ORDERING_KEY
   }

  // Imports the Google Cloud client library
  const {PubSub} = require('@google-cloud/pubsub');
    
  const apiEndpoint = region + '-pubsub.googleapis.com:443';

  console.log("Connecting to pubsub on " + apiEndpoint);

  // Creates a client; cache this for further use
  const pubSubClient = new PubSub({
    // Sending messages to the same region ensures they are received in order
    // even when multiple publishers are used.
    apiEndpoint: apiEndpoint,
    projectId: process.env.PROJECT_ID,
  });

  const os = require('os')
  const subscriptionName = "c-ordered-" + region;

  async function createSubscriptionWithOrdering() {
    // Creates a new subscription
    subscription = await pubSubClient.topic(topicName).createSubscription(subscriptionName, {
      enableMessageOrdering: true,
    });
    console.log(
      `Created subscription ${subscriptionName} with ordering enabled.`
    );
    console.log(
      'To process messages in order, remember to add an ordering key to your messages.'
    );

    return subscription;
  }

  async function getSubscription() {
    // Gets the metadata for the subscription
    const subscription = await pubSubClient
      .subscription(subscriptionName)
    const [metadata] = await subscription.getMetadata();

    console.log(`Subscription: ${metadata.name}`);
    console.log(`Topic: ${metadata.topic}`);
    console.log(`Push config: ${metadata.pushConfig.pushEndpoint}`);
    console.log(`Ack deadline: ${metadata.ackDeadlineSeconds}s`);

    return subscription;
  }

  function listenForMessages(subscription) {
    // Create an event handler to handle messages
    let messageCount = 0;
    let lastMessageNum = -1;
    const messageHandler = message => {
      console.log(`Received message ${message.id}: Data: ${message.data}`);
      messageCount += 1;

      const messageBody = JSON.parse(message.data);
      if (lastMessageNum > messageBody.messageNum) {
        console.log("out of order message detected! " + messageBody.messageNum + " < " + lastMessageNum);
      }

      lastMessageNum = messageBody.messageNum;


      // "Ack" (acknowledge receipt of) the message
      message.ack();
    };

    // Listen for new messages until timeout is hit
    subscription.on('message', messageHandler);
    var timeout = 60;

    /*
    setTimeout(() => {
      subscription.removeListener('message', messageHandler);
      console.log(`${messageCount} message(s) received.`);
    }, timeout * 1000);
    */
  }


  //createSubscriptionWithOrdering().catch(console.error);
  var subscription;
  subscription = await getSubscription().catch(async () => {
      subscription = await createSubscriptionWithOrdering();
  });

  // start consuming messages
  listenForMessages(subscription);


}

process.on('unhandledRejection', err => {
  console.error(err.message);
  process.exitCode = 1;
});
main(...process.argv.slice(2));