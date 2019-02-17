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

 * Use the Elasticsearch API to determine the number of shards that back a specific index if the user doesn't specify
 a degree of parallelism.  If an alias or wildcard index is used, all shards that match will be returned.  If there isn't
 a lot of data in each shard it might be more efficient to use a minimal number of slices reducing the total number of round trips.
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
        *

