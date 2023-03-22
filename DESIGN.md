# Autobase Design

This document outlines the design of an algorithm that achieves eventual consistent ordering of a distributed set of message from multiple writers.

## Overview

A message is constructed as a node in a Directed Acyclic Graph (DAG). The message includes a clock which contains a causal reference to other nodes in the graph. At any one point, a deterministic linearisation may be achieved by traversing the graph according to clocks

## Linearisation

For example, given the following messages from 3 writers: `a`, `b`, `c`

```
a0 - b0 - c0 - a1 - b1
```

Trivially, the linearisation would be:

```
[a0, b0, c0, a1, b1]
```

If we introduce branching: eg.:

```
a0 - c0 - a1
   /
b0
```

With no other information, 2 linearisations are possible:

```
[a0, b0, c0, a1]
```

or

```
[b0, a0, c0, a1]
```

This can be deterministically sorted by key comparison.

This may be applied recursively to resolve more complex dags:

```
a < b < c

a0   c0 
 | X |
b0   a1
 | \ | 
c1   b1
 | /
b2
```

May be linearised as:

```
[a0, c0, a1, b0, b1, c1, b2]
```

## Consistency 

While a deterministic linearisation may always be made, there is no guarantee of consistency between peers. For example, in the above DAG, writer `c` would have the linearised view of:

```
[a0, c0, b0, c1]
```

It turns out, through counting the number of writers that reference each node, we are able to determine when consistency is guaranteed.

We use the term __vote__ to denote a reference from a writer to a node. Like this, each node in the graph accumulates __votes__ from other writers (max 1 per writer), and these votes can be used as a metric to determine consistent ordering.  

### Quorums

In order to express a condition, we define the concept of __quorums__ over nodes:

- A node achieves a __quorum__ once it has been referenced by a majority of writers, or equivalently once it has a majority of votes. 

We recursively define the degree of a quorum. A quroum increases in degree once the quorum has itself been referenced by a majority of writers (ie. a majority of writers are aware of a majority over a given node)

For example, A 2nd degree quorum or __double quorum__ is achieved once it is deduced that a majority of writers are themselves aware of a majority over a given node.

Consider the DAG:
```
       2' quorum
           |
a0 - b0 - c0 - a1
     |         |
  1' quorum  3' quorum
```

- `b0` forms a quorum over `a0` since writers `a` & `b` have referenced `a0`
- `c0` forms a double quorum over `a0`, since writers `b & c` have referenced the quorum at `b0`
- likewise, `a1` forms a triple quorum over `a0`

### Condition for Consistency

A single quorum is insufficient to ensure consistency. Consider the following DAG:

```
writers: { a < b < c }

*a0  *c0
 |  / |
 c1   b0

quroums:
 a0 <= { a, c }
 c0 <= { b, c }

linearised view:

a [a0]
b [c0, b0]
c [a0, c0, c1]

```

The nodes `a0` and `c0` have both achieved quorums independently. Despite being part of the same quorum over `c0`, writers `b` & `c` have conflicting linearisations, and `b`s view would be rebased once it synced `a`s feed.

The conflict comes about because `c` is part of both quorums, and therefore cast contributed conflicting votes to either.

In general, this condition will __always__ hold true:

- Any two quorums __must__ have at least one writer in common

We need a stronger condition for consistency. Let's consider higher quorums.

#### Higher Quorums

```
c0
|
b0
|
c1

quorums:

c0 <= { b, c }
b0 <= { b, c }

double quorums:

c0 <= { b, c }

```

Now a double quorum has been achieved: a majority of writers have seen the single quorum over `c0`. We know that any other majority formed after the double quorum must have a common member. Therefore, that majority must also be aware of the single quorum over `c0`.

- The double quorum over `c0` implies that no other majority can form without including a reference to the single quorum over `c0`.

This is a powerful statement, since it guarantees that all other quorums will have strictly more information about the graph than we did at `b0`. As such, the graph can be consistently ordered up to `b0` by every writer in the system.

#### Caveat

The above condition is almost complete, but it needs one additional statement. It turns out a double quorum is not sufficient on it's own. If a separate quorum is also referenced, then it is possible for that quorum to also have achieved a double quorum, without our double quorum being aware of it.

Therefore we can only guarantee consistent ordering up to a given node once a quorum is achieved over that node that is 2 degrees higher than any other quorum that has been referenced.

If that condition is achieved, no contending quorum is able to surpass this quorum, since any votes the contender accumulates will also reference this quorum.

## Consistent Ordering

The DAG must be ordered in a specific way to be consistent. We must ensure that our ordering will be consistent for any actions taken by writers not in this quorum, ie. ordering against the worst case scenario.

Consider the following DAG:

```
3 writers: { a, b, c }

a0  b0
|   |
c0  |
| \ |
a1  c1
    |
    b1

```

In this case, if writers `b` & `c` continue cooperating they will be able to confirm `c1` and order the graph below.

With `a1` writer `a` sees a double quroum over `a0`, with no competing quorums and therefore is able to lock in `a0`. 

`b` & `c` must anticipate this and always order `a0` before `b0`.

This follows from the statement made earlier, that a double-lead quorum implies that any other quorum has more information about the graph. `c1` references the preceeding quorum and as a result, `b` and `c` are able to order the graph under the worst-case assumption that this quorum has achieved a 2 point lead that they are unaware of.

### Tails, Forks and Merges

// todo
// if branches are cross-linked and end up referencing the same tails then we can yield
// if we see a merge, then we should wait for the merge point to be yielded before ordering

## Writer Roles

There are 3 roles a writer can take:

1. Indexing Writer
2. Non-indexing Writer
3. Relayed Writer

### Indexing Writer

Indexing writers are writers whose references count towards quorums. In the above sections, we have exclusively been dealing with indexing writers.

### Non-indexing Writer

Non-indexing writers function the same as indexing writers, but they do not count towards quorums. This allows for control structures, eg. an indexing server with clients as non-indexing writers.

All clients can submit entries, but the server alone determines the ordering of the linearized view.

### Relayed Writer

A relayed writer is a writer whose entries can never be the head of the DAG. This is to say that entries from a relayed writer will only be included in the graph if they are referenced by a confirmed node from a (non-)indexing writer.