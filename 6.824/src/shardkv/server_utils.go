package shardkv

import (
	"bytes"

	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
)

func (sc *ShardKV) Is_Duplicate(client_id int64, request_id int, shard_id int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	last_request_id, exist := sc.kv_database[shard_id].Last_request_id[client_id]
	if !exist {
		return false // 如果kv的记录中不存在这个client id，说明这个request一定不会重复
	}
	// 如果kv的记录中存在这个client id，那么这个client保存的最后一个requestid要小于当前request，才不会重复
	return last_request_id >= request_id
}

func (sc *ShardKV) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if sc.rf.GetRaftStateSize() > (sc.maxraftstate * proportion / 10) {
		// Send SnapShot Command
		snapshot := sc.MakeSnapShot()
		sc.rf.Snapshot(raftIndex, snapshot)
	}
}

// Handler the SnapShot from kv.rf.applyCh
func (sc *ShardKV) GetSnapShotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		sc.ReadSnapShotToInstall(message.Snapshot)
		sc.last_snapshot_log_index = message.SnapshotIndex
	}
}

// Give it to raft when server decide to start a snapshot
func (sc *ShardKV) MakeSnapShot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.kv_database)
	e.Encode(sc.ctrl_config)
	e.Encode(sc.migrating_shard)
	data := w.Bytes()
	return data
}

func (sc *ShardKV) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_kvshard []Shard
	var persist_config shardctrler.Config
	var persist_migrating_shard [shardctrler.NShards]bool

	if d.Decode(&persist_kvshard) != nil || d.Decode(&persist_config) != nil || d.Decode(&persist_migrating_shard) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", sc.me)
	} else {
		sc.kv_database = persist_kvshard
		sc.ctrl_config = persist_config
		sc.migrating_shard = persist_migrating_shard
	}
}
