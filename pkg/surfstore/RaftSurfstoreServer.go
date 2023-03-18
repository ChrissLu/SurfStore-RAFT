package surfstore

import (
	context "context"
	"fmt"
	//"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	serverId       int64
	commitIndex    int64
	pendingCommits []*chan bool
	lastApplied    int64
	peers          []string
	//ip             string

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// waiting for majority to be alive
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	//fmt.Println(11111111111)
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// waiting for majority to be alive
	for {
		success, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if success.Flag {
			break
		}
	}
	//fmt.Println(2222222222)
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// waiting for majority to be alive
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}
	//fmt.Println(3333333)
	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel

	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** use for loop in func sendToFollower
	fmt.Println(444444)
	// commit the entry once majority of followers have it in their log
	commit := <-commitChan
	fmt.Println(5555555)
	// once committed, apply to the state machine
	if commit {
		s.lastApplied = s.commitIndex // s.lastApplied not needed for leader?
		fmt.Println(6666666)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	//in case of another client call updateFile before this goroutine finished
	targetInd := s.commitIndex + 1
	pendingInd := len(s.pendingCommits) - 1

	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.serverId {
			continue
		}
		go s.sendToFollower(ctx, targetInd, addr, responses)
	}

	totalAppends := 1

	// wait in loop for responses
	for {
		println("in loop")
		result := <-responses
		if result {
			totalAppends++
		}
		println("totalAppends",totalAppends)
		if totalAppends > len(s.peers)/2 {
			// put on corresponding channel
			println("put on corresponding channel")
			*s.pendingCommits[pendingInd] <- true
			// update commit Index correctly
			s.commitIndex = targetInd
			break
		}
	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, targetInd int64, addr string, responses chan bool) {
	count:=0
	for { //keep trying
		AppendEntriesInput := AppendEntryInput{
			Term: s.term,
			// put the right values
			PrevLogIndex: targetInd - 1,
			PrevLogTerm:  -1,
			Entries:      s.log[:targetInd+1],
			LeaderCommit: s.commitIndex,
		}
		if AppendEntriesInput.PrevLogTerm > 0 {
			AppendEntriesInput.PrevLogTerm = s.log[AppendEntriesInput.PrevLogTerm].Term
		}

		// check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			if count == 0{
				fmt.Println(err)
			}
			count+=1
			continue
		}
		client := NewRaftSurfstoreClient(conn)

		ctx,cancel :=context.WithTimeout(context.Background(),time.Second)
		defer cancel()
		//print(9)
		output, err := client.AppendEntries(ctx, &AppendEntriesInput)
		if err != nil {
			//print(9)
			if count == 0{
				fmt.Println(err)
			}
			count+=1
			continue
		}
		//println(9)
		//  check output
		if output!=nil&& output.Success {
			responses <- true
			break
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		//ServerId:     s.serverId,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	if s.isCrashed {
		return output, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}
	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term > s.term {
		return output, fmt.Errorf("append from leader with wrong term")
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm (§5.3)

	// if len(s.log) <= int(input.PrevLogIndex) {
	// 	return output, fmt.Errorf("log does not contain an entry at prevLogIndex")
	// } else 
	if input.PrevLogIndex > -1 &&len(s.log) > int(input.PrevLogIndex)&& s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		return output, fmt.Errorf("wrong PrevLogTerm")
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)
	for ind, entry := range s.log {
		if ind > len(input.Entries)-1 {
			s.log = s.log[:ind]
			input.Entries = make([]*UpdateOperation, 0)
			break
		}
		if entry != input.Entries[ind] {
			s.log = s.log[:ind]
			input.Entries = input.Entries[ind:]
			break
		}
		if ind == len(s.log)-1 { //all match
			input.Entries = input.Entries[len(s.log):]
		}
	}

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit < int64(len(s.log)-1) {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(len(s.log) - 1)
		}
	}

	//Apply to state machine
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}
	output.Success = true
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	// for i:=0;i<len(s.peers);i++{
	// 	if
	// }

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	AppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      make([]*UpdateOperation, 0),  //do not use heartbeat to send log
		LeaderCommit: s.commitIndex,
	}
	// contact all the follower, send some AppendEntries call

	aliveServers := 0
	for idx, addr := range s.peers {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue //should't break the loop if one of followers failed
		}
		client := NewRaftSurfstoreClient(conn)

		ctx,cancel :=context.WithTimeout(context.Background(),time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, &AppendEntriesInput)
		if err != nil {
			continue
		}
		if output != nil {
			aliveServers++
		}
	}
	if aliveServers <= len(s.peers)/2 {
		return &Success{Flag: false}, fmt.Errorf("failed to send heartbeats to majority")
	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
