# SESDAD

This project aims at implementing a simplified (and therefore far from complete) implementation of a reliable, distributed message broker supporting the publish-subscribe paradigm (in Portuguese, "Sistema de Edição-Subscrição", SES).

The publish-subscribe system we are aiming at involves 3 types of processes: publishers,
subscribers, and message brokers. Publishers are processes that produce events on one or
more topics. Multiple publishers may publish events on the same topic. Subscribers register
their interest in receiving events of a given set of topics. Brokers organize themselves in a
overlay network, to route events from the publishers to the interested subscribers. Communication
among publishers and subscribers is indirect, via the network of brokers. Both
publishers and subscribers connect to one broker (typically, the \nearest" broker in terms of
network latency) and send/receive events to/from that broker. Brokers coordinate among
each other to propagate events in the overlay network.

More information in [Project Report](https://github.com/carlosfaria94/SESDAD/blob/master/Report.pdf) or [Project statement](https://github.com/carlosfaria94/SESDAD/blob/master/DAD_Project-1516.pdf).

## Event Routing

- <b>Flooding</b>: In this approach events are broadcast across the tree.
- <b>Filtering based</b>: In this approach, events are only forwarded only along paths leading
to interested subscribers. To this end, brokers maintain information regarding which
events should be forwarded to their neighbours.

More information in [Project Report](https://github.com/carlosfaria94/SESDAD/blob/master/Report.pdf) or [Project statement](https://github.com/carlosfaria94/SESDAD/blob/master/DAD_Project-1516.pdf).

## Ordering guarantees

- <b>Total order</b>: all events published with total order guarantees are delivered in the same
order at all matching subscribers. More formally, if two subscribers s1; s2 deliver
events e1; e2, s1 and s2 deliver e1 and e2 in the same order. This ordering property
is established on all events published with total order guarantee, independently of the
identity of the producer and of the topic of the event.
- <b>FIFO order</b>: all events published with FIFO order guarantee by a publiser p are delivered
in the same order according to which p published them.
- <b>No ordering</b>: as the name suggests, no guarantee is provided on the order of notification
of events.

More information in [Project Report](https://github.com/carlosfaria94/SESDAD/blob/master/Report.pdf) or [Project statement](https://github.com/carlosfaria94/SESDAD/blob/master/DAD_Project-1516.pdf).

## Contributions

NOTE: This project is a culmination of work of the following members:

- Carlos Faria <carlosfigueira@tecnico.ulisboa.pt>
- Sérgio Mendes <sergiosmendes@tecnico.ulisboa.pt>
- Nuno Vasconcelos <nunovasconcelos@tecnico.ulisboa.pt>

