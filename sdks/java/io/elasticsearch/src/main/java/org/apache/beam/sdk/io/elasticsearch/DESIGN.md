ElasticsearchIO Splittable DoFn Implementation Proposal
=======================================================


PTransform Transition
---------------------

1. Implement an SDF which takes a JSON string, executes the search and outputs a JSON string for each result.
1. Move re-usable code to a utility class to help with the transition.
1. Implement a ReadAll transform which uses the SDF and make available to users as ElasticsearchIO.ReadAll
1. Work on supporting SDF across the core runners.
1. Transition ElasticsearchIO.Read to use the new SDF implementation removing the Source API implementations.
1. Refactor the utility class to reduce code sprawl.


SDF Implementation Specifics
----------------------------

Currently the Source API implementation supports parallelism via the the Elasticsearch Slice API.  This enables
maximum parallelism of the total number of shards available to store the data.

### Parallelism by Default ###

Using the Elasticsearch API we can determine the number of shards that back a specific index if the user doesn't specify
a degree of parallelism.  If an alias or wildcard index is used all shards for matching indices will be counted.  This 
represents the maximum degree of parallelism that can be achieved however doesn't necessarily represent the optimal amount.
Queries that match a large amount of data per shard can take advantage however resource usage of the cluster will increase
dramatically potentially making it less available for other users.  If there isn't a lot of data in each shard it might 
be more efficient to use a minimal number of slices reducing the total number of round trips. The ideal amount of 
parallelism is going to be a function of the total number of shards, number and size of client nodes, and total number 
of results matched.  

One simple proposal for managing this parallelism is to use `max(shardCount, maxSlices)` with maxSlices defaulting to 4.
Work could be done for the processing of a single element to keep track of the total number of results and use that information
when the runner decides to re-balance the work.

### Initial Restriction & Restriction Splitting ###

When determining the initial restriction, the maximum upper bound is the result of the maximum level of parallelism
when using a version of Elasticsearch that supports the Slice API.

    if (backendVersion < 5) {
      return new OffsetRange(0L, 1L);
    }
    return new OffsetRange(0L, numSlices);
    
This range represents each slice that will be available for the query and work will be split in half initially.  This
ensures 
 
 
 * The SDF will take a string as input and output strings.  This means there is no type safety to guarantee the input is JSON.
 * Leverage composition instead of providing a function to transform input elements to queries within the SDF.
 * Contrary to the HBase SDF implementation, it seems better to attempt the claim of work before execution instead of after because
 the runner could have split the restriction beforehand and assigned the work to another worker effectively querying the data multiple
 times.
   * Q: Is it better to retrieve the data before attempting to claim the work?
   Seems like it will risk multiple workers attempting the work taxing the data source.
   Could returning ProcessContinuation.resume() reduce the risk of a restriction being split and extra work being done?
   * Q: Is it better to try claiming the work before execution?
   This will ensure there is only a second read upon failure.
   What failure modes will we observe if we go this route?
   If runners are able to observe an exception during the processing of a slice and retry appropriately this might be the best solution.

### Questions ###

Guidance on how tryClaim and restriction splitting is unclear for pipeline writers.
 * Which is more correct?
    * Returning ProcessContinuation.resume() after processing one work item.
    * Looping through the known restriction range until complete or tryClaim fails.
 * When should tryClaim be called?  Both options seem to introduce failure conditions that must be handled.
    * Before the work has been completed.
        * If a failure occurs the runner must be aware and reschedule the offset. Documentation makes this seem like the most stable approach.
    * After the work has been completed.
        * The HBase implementation works in this fashion.
        * Because tryClaim is called after the results are output, it's not possible to tell if another worker has already output the work.
        In this case tryClaim will return false so data should be available however the overhead seems pretty high.
 * Sometimes processElement gets called with `OffsetRangeTracker{range=[1, 1)` which shouldn't happen and tells me
 the current implementation isn't quite right.
 * Would users like parallelism to happen automatically?
    * Based on number of shards?
    * Number of configured client nodes?
    * A function of number of shards?
    * None?
 * Configuring a maximum amount of parallelism could help users control the load on their cluster(s).  `withMaxSlices()`?
 *  

### Test Cases ###

1. Retrieves all documents via `match_all` when a null query is provided.
1. Restriction splitting accurately creates two approximately equal restrictions.
1. Test that scrolling works
1. Ensure multiple slices returns the expected results