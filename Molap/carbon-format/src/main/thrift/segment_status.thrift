enum SegmentState {
	LOAD_PROGRESS=0; // Loading is in progress
	SUCCESS=1 //Data is in readable state (successful load/update)
	DELETE_PROGRESS=2;	// Data is in process of deletion
	UPDATE_PROGRESS=2;	// Data is in process of update
}

struct SegmentStatus{
	1: SegmentState current_state // The current status of the segment
	2: i64 creation_time // The last creation time
	3: i64 latest_state_time // The time when last state was changed
}