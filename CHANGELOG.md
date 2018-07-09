Changelog for queue-messaging
=================

0.3.1 (2018-07-09)
------------------

- Freeze versions of required packages.


0.3.0 (2018-06-29)
------------------
- Backward incompatible update for latest google-cloud-pubsub compatiblity.
   - PubSub message pulling becomes asynchronous
   - messaging.receive instead of returning an Envelope with message now takes a callback to be called on Envelope
   - google-cloud-pubsub will manage sleeping when pulling for new messages on its own
   - Google Cloud project id is now required for pubsub configuration


0.2 (2017-05-17)
----------------

- Set not provided and not required fields to None.
- Removed pytest from dependencies.
- Use HTTP instead of gRPC by default, allow to pick gRPC instead in configuration.


0.1.0 (2017-01-25)
------------------

- First release.
- Simple high-level, provider independent API for receiving and sending structured messages.
- Validation when serializing data - protection against sending invalid data.
- Retry logic in case of connection error.
- Dead letter queue support. Send messages to another queue in case of an error.
- Custom MACAddressField for schema. 
