package surfstore

import (
	context "context"
	"fmt"
	"sync"

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

	id             int64
	commitIndex    int64
	pendingCommits []*chan bool
	lastApplied    int64
	peers          []string
	ip             string

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	panic("todo")
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if s.isLeader {
		return s.metaStore.GetBlockStoreMap(ctx, hashes)
	} else {
		return nil, ERR_NOT_LEADER
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if s.isLeader {
		return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	} else {
		return nil, ERR_NOT_LEADER
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel

	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies

	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppends > len(s.peers)/2 {
		// TODO put on correct channel
		*s.pendingCommits[0] <- true
		// TODO update commit Index correctly
		s.commitIndex = 0
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	// TODO check all errors
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	_, _ = client.AppendEntries(ctx, &dummyAppendEntriesInput)

	// TODO check output
	responses <- true

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
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}

	// TODO actually check entries
	s.log = input.Entries

	for s.lastApplied < input.LeaderCommit {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	return nil, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// TODO update state
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
		Entries:      make([]*UpdateOperation, 0), // do not use heartbeat to send log
		LeaderCommit: s.commitIndex,
	}
	// contact all the follower, send some AppendEntries call

	aliveServers := 0
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue //should't break the loop if one of followers failed
		}
		client := NewRaftSurfstoreClient(conn)

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
