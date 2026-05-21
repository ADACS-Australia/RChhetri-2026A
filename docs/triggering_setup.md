# Triggering Setup

The pipeline triggering system uses three moving parts:

- The _Watcher_, which polls a data source for new observations
- The _Courier_, which moves data from the source to a working directory
- The pipeline itself, a Prefect deployed flow that processes the incoming data

These three components work together to make a triggering system

![Event Driven Triggering](./images/event_driven_triggering.png)
