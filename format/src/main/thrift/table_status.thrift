/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * File format description for carbon table status file
 */
namespace java org.apache.carbondata.format

enum SegmentState {
	LOAD_PROGRESS = 0; // Loading is in progress
	SUCCESS = 1; //Data is in readable state (successful load/update)
	DELETE_PROGRESS = 2;	// Data is in process of deletion
	UPDATE_PROGRESS = 3;	// Data is in process of update
}

struct SegmentStatus{
	1: SegmentState current_state; // The current status of the segment
	2: i64 creation_time; // The last creation time
	3: i64 latest_state_change_time; // The time when last state was changed
}
