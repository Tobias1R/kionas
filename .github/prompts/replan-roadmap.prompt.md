
## ROADMAPPING
With the end of phase 6 we have achieved a full loop on our ecosystem. Client->Server->Worker->flight->client. But theres a lot to be done around this same cycle. Lets take a higher leve example. select order by is not working. We could implement this in the worker, but we need to make sure the server can parse and plan it, and that the flight proxy can handle the new query handle metadata. So we have to do some work in all 3 places to get this working. This is what we will be doing in phases 7-11. We will be iterating on this cycle of client->server->worker->flight->client for each new feature we want to add. But now that we have more context and the project is taking shape, we can re-plan a little bit to make sure we are building the right things in the right order. So i'm thinking in creating a new roadmap file that outlines the next phases in more detail and with a better understanding of the dependencies between them. This will help us stay focused and make sure we are building the right things in the right order. I'll start by outlining the next few phases and then we can iterate on it together to make sure it makes sense and covers everything we want to achieve.

# Roadmap 2
## Phase 1: Replan roadmap and set up for next phase
- Create a new roadmap file that outlines the next phase in more detail and with a better understanding of the dependencies between them.
- Starting points: 
  - We have a full loop on our ecosystem. Client->Server->Worker->flight->client.
  - We have a basic query dispatch path for select statements.
  - We have a basic worker implementation that can execute simple queries and return results.
  - We have a basic flight proxy implementation that can route queries to workers and return results to clients.
- We have a basic auth implementation that can authenticate clients and propagate tokens to workers.
- We have a basic logging implementation that can log query execution and results.
- Deltatables are being created and will be the basis for our logical plan model and execution engine.

### What we want to achieve in the next phases:
1. Expand query federation and advanced semantics iteratively with feature flags.
2. Server session expansion to support more complex query contexts and state management.
3. Query expansion to support more complex SQL features like order by, qualify, window functions, etc.
4. Update and delete support to allow for data modification operations.

#### Based on that
We need to improve planning and planner! We need to make sure we have a solid logical plan model that can support the new features we want to add. We also need to make sure we have good validation and explain output to help us debug and optimize our queries. This will be a continuous effort that will run in parallel with the other phases, but it will deepen after each phase as we add more complex features. 

### Discovery:
- put on the discovery section of the PHASE 2 what are the main blockers and requirements to improve the planner.
- some strategies required to achieve a good planner(like Indexing, statistics, cost model, etc). can be extremelly complex, So lets not dive into the details of each one, but we need to list them as important components of a good planner.
- About architectural decisions:
  - present them in the discovery section as well, but we can have a separate section for them in the roadmap file. This will help us keep track of the decisions we make and the rationale behind them. We can also use this section to document any changes we make to the architecture as we iterate on the roadmap.

## Phase 2: The Discovery