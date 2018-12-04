#!/bin/sh
#32 minutes
CATEGORY="MultiDynamoDBStoreTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#22 minutes
CATEGORY="IsolateMultiVertexCentricQuery"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#17.5 minutes
CATEGORY="SingleDynamoDBGraphTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#17.3 minutes
CATEGORY="SingleDynamoDBStoreTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#16.5 minutes
CATEGORY="IsolateMultiLargeJointIndexRetrieval"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#15.7 minutes
CATEGORY="MultiDynamoDBOLAPTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#13.5 minutes
CATEGORY="MultiDynamoDBGraphTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#11.8 minutes
CATEGORY="IsolateMultiEdgesExceedCacheSize"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#12 minutes
CATEGORY="SingleIdAuthorityLogStoreCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#11 minutes
CATEGORY="MultiIdAuthorityLogStoreCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#9 minutes
CATEGORY="SingleDynamoDBOLAPTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#8.7 minutes
CATEGORY="IsolateMultiConcurrentGetSliceAndMutate"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#8.5 minutes
CATEGORY="IsolateRemainingTestsCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#6.5 minutes
CATEGORY="IsolateMultiConcurrentGetSlice"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#6.4 minutes
CATEGORY="IsolateSingleConcurrentGetSliceAndMutate"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#4.8 minutes
CATEGORY="IsolateSingleConcurrentGetSlice"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#3.2
CATEGORY="SingleDynamoDBMultiWriteStoreTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#2.8 minutes
CATEGORY="MultiDynamoDBMultiWriteStoreTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
#To be added
CATEGORY="GraphSimpleLogTestCategory"
mvn install -P integration-tests -Dgroups="com.amazon.janusgraph.testcategory.${CATEGORY}" -Dinclude.category="**/*.java"
