package surfstore

import (
	context "context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore   *MetaStore
	ID          int64
	peers       []string // include myself
	commitIndex int64
	lastApplied int64
	matchIndex  []int64 // initialized to all -1s
	nextIndex   []int64 // initialized to all 0s
	// pendingCommits []*chan bool

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	log.Printf("Server[%v]: GetFileInfoMap\n", s.ID)
	s.isCrashedMutex.RLocker().Lock()
	if s.isCrashed {
		log.Printf("Server[%v]: GetFileInfoMap crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.RLocker().Lock()
	if !s.isLeader {
		log.Printf("Server[%v]: GetFileInfoMap not the leader\n", s.ID)
		s.isLeaderMutex.RLocker().Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RLocker().Unlock()

	majorityChan := make(chan bool)

	log.Printf("Server[%v]: GetFileInfoMap SendToAllFollowers\n", s.ID)
	go s.SendToAllFollowers(ctx, &majorityChan)

	for connected := <-majorityChan; !connected; { // blocking here
		log.Printf("Server[%v]: GetFileInfoMap failed to contact majority of the nodes, retry\n", s.ID)
		time.Sleep(2 * time.Second)
		go s.SendToAllFollowers(ctx, &majorityChan)
	}
	log.Printf("Server[%v]: GetFileInfoMap return right value\n", s.ID)
	rst, err := s.metaStore.GetFileInfoMap(ctx, empty)
	if err != nil {
		return nil, err
	}
	return rst, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	log.Printf("Server[%v]: GetBlockStoreMap\n", s.ID)
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		log.Printf("Server[%v]: GetBlockStoreMap crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.RLocker().Lock()
	if !s.isLeader {
		s.isLeaderMutex.RLocker().Unlock()
		log.Printf("Server[%v]: GetBlockStoreMap not the leader\n", s.ID)
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RLocker().Unlock()

	majorityChan := make(chan bool)

	log.Printf("Server[%v]: GetBlockStoreMap SendToAllFollowers\n", s.ID)
	go s.SendToAllFollowers(ctx, &majorityChan)

	for connected := <-majorityChan; !connected; { // blocking here
		log.Printf("Server[%v]: GetBlockStoreMap failed to contact majority of the nodes, retry\n", s.ID)
		time.Sleep(2 * time.Second)
		go s.SendToAllFollowers(ctx, &majorityChan)
	}
	log.Printf("Server[%v]: GetBlockStoreMap return right value\n", s.ID)
	rst, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
	if err != nil {
		return nil, err
	}
	return rst, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	log.Printf("Server[%v]: GetBlockStoreAddrs\n", s.ID)
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		log.Printf("Server[%v]: GetBlockStoreAddrs crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.RLocker().Lock()
	if !s.isLeader {
		log.Printf("Server[%v]: GetBlockStoreAddrs not the leader\n", s.ID)
		s.isLeaderMutex.RLocker().Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RLocker().Unlock()

	majorityChan := make(chan bool)

	log.Printf("Server[%v]: GetBlockStoreAddrs SendToAllFollowers\n", s.ID)
	go s.SendToAllFollowers(ctx, &majorityChan)

	for connected := <-majorityChan; !connected; { // blocking here
		log.Printf("Server[%v]: GetBlockStoreAddrs failed to contact majority of the nodes, retry\n", s.ID)
		time.Sleep(2 * time.Second)
		go s.SendToAllFollowers(ctx, &majorityChan)
	}
	log.Printf("Server[%v]: GetBlockStoreAddrs update commitIndex\n", s.ID)
	rst, err := s.metaStore.GetBlockStoreAddrs(ctx, empty)
	if err != nil {
		return nil, err
	}
	return rst, nil
}

//  1. check isLeader and isCrashed, append the filemeta to the log
//  2. send AppendEntries() to all the followers
//     2.1 send log[nextIndex[peerIndex]: ] prevIndex = nextIndex[peerIndex] - 1, ** check boundary **
//     2.2 if return false because of inconsistency, retry with smaller nextIndex
//     2.3 if return false becasue of wrong term, check the term and convert to follower, update current term and return error
//  3. block until majority of the followers return true, commitChan
//  4. apply to metaStore, update lastApplied and commitIndex
//  5. update matchIndex to the return value, update nextIndex matchIndex + 1
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	log.Printf("Server[%v]: UpdateFile\n", s.ID)
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RLocker().Unlock()
		log.Printf("Server[%v]: UpdateFile crashed\n", s.ID)
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.RLocker().Lock()
	if !s.isLeader {
		s.isLeaderMutex.RLocker().Unlock()
		log.Printf("Server[%v]: UpdateFile not the leader\n", s.ID)
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RLocker().Unlock()

	s.log = append(s.log, &UpdateOperation{Term: s.term, FileMetaData: filemeta})
	commitChan := make(chan bool)

	log.Printf("Server[%v]: UpdateFile SendToAllFollowers\n", s.ID)
	go s.SendToAllFollowers(ctx, &commitChan)

	if commit := <-commitChan; !commit { // blocking here
		log.Printf("Server[%v]: UpdateFile failed to contact majority of the nodes, retry\n", s.ID)
		time.Sleep(2 * time.Second)
		go s.SendToAllFollowers(ctx, &commitChan)
	} else {
		log.Printf("Server[%v]: UpdateFile update commitIndex\n", s.ID)
		s.commitIndex = int64(len(s.log) - 1)
		rst := &Version{}
		var err error
		for s.lastApplied < s.commitIndex {
			rst, err = s.metaStore.UpdateFile(ctx, s.log[s.lastApplied+1].FileMetaData)
			s.lastApplied += 1
		}
		if err != nil {
			return &Version{
				Version: -1,
			}, err
		}
		return rst, nil
	}
	time.Sleep(10 * time.Second)
	return nil, ERR_MAJORITY_DOWN
}

func (s *RaftSurfstore) SendToAllFollowers(ctx context.Context, commitChan *chan bool) {
	responses := make(chan bool, len(s.peers)-1)
	for i := range s.peers {
		if int64(i) == s.ID {
			continue
		}
		go s.SendToFollower(ctx, i, &responses)
	}

	totalResponses := 1
	totalSuccess := 1
	for response := range responses {
		totalResponses += 1
		if response {
			totalSuccess += 1
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalSuccess > len(s.peers)/2 {
		log.Printf("Server[%v]: Connected to majority of the followers\n", s.ID)
		*commitChan <- true
	} else {
		*commitChan <- false
	}
}

// send AppendEntries to a follower, send fasle to response if crashed, otherwise retry until success
// update matchIndex to the return value, update nextIndex matchIndex + 1
func (s *RaftSurfstore) SendToFollower(ctx context.Context, peerIndex int, responses *chan bool) {
	conn, err := grpc.Dial(s.peers[peerIndex], grpc.WithInsecure())
	if err != nil {
		*responses <- false
		return
	}
	client := NewRaftSurfstoreClient(conn)
	for {
		var prevLogTerm int64 = 0
		if s.nextIndex[peerIndex] != 0 {
			prevLogTerm = s.log[s.nextIndex[peerIndex]-1].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: s.nextIndex[peerIndex] - 1,
			PrevLogTerm:  int64(prevLogTerm),
			Entries:      s.log[s.nextIndex[peerIndex]:],
			LeaderCommit: s.commitIndex,
		}
		output, err := client.AppendEntries(ctx, input)
		if err != nil { // follower crashed, don't retry
			log.Printf("Server[%v]: Follower[%d] crashed, response false\n", s.ID, peerIndex)
			*responses <- false
			return
		}
		if output.Success { // follower commited
			log.Printf("Server[%v]: Follower[%d] commited, response true\n", s.ID, peerIndex)
			s.nextIndex[peerIndex] = int64(len(s.log))
			s.matchIndex[peerIndex] = output.MatchedIndex
			*responses <- true
			return
		} else if output.Term != s.term { // TODO: is possible?
			log.Printf("Server[%v]: Follower[%d] term mismatch?\n", s.ID, peerIndex)
			s.term = output.Term
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
			*responses <- false
			return
		}
		if s.nextIndex[peerIndex] != 0 {
			s.nextIndex[peerIndex] -= 1
		}
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Printf("Server[%v]: AppendEntries\n", s.ID)
	s.isCrashedMutex.RLocker().Lock()
	if s.isCrashed {
		log.Printf("Server[%v]: AppendEntries crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	if s.term > input.Term {
		log.Printf("Server[%v]: AppendEntries wrong term\n", s.ID)
		return &AppendEntryOutput{
			ServerId:     s.ID,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	if s.term < input.Term {
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}
	if input.PrevLogIndex != -1 && (input.PrevLogIndex > int64(len(s.log)-1) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		log.Printf("Server[%v]: AppendEntries log mismatch\n", s.ID)
		return &AppendEntryOutput{
			ServerId:     s.ID,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}
	s.log = s.log[:input.PrevLogIndex+1]
	s.log = append(s.log, input.Entries...)
	log.Printf("Server[%v]: AppendEntries updated log: %v\n", s.ID, s.log)
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit <= int64(len(s.log)-1) {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(len(s.log) - 1)
		}
		log.Printf("Server[%v]: AppendEntries updated commitIndex: %v\n", s.ID, s.commitIndex)
		for s.lastApplied < s.commitIndex {
			_, err := s.metaStore.UpdateFile(ctx, s.log[s.lastApplied+1].FileMetaData)
			if err != nil {
				log.Printf("Server[%d]: AppendEntries updatefile failed\n", s.ID)
				return nil, err
			}
			s.lastApplied += 1
		}
	}
	log.Printf("Server[%v]: AppendEntries updated lastApplied: %v\n", s.ID, s.lastApplied)
	return &AppendEntryOutput{
		ServerId:     s.ID,
		Term:         s.term,
		Success:      true,
		MatchedIndex: int64(len(s.log) - 1),
	}, nil
}

// return ERR_SERVER_CRASHED if crashed, update isLeader, increment term, update nextIndex and matchIndex
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Printf("Server[%v]: SetLeader \n", s.ID)
	s.isCrashedMutex.RLocker().Lock()
	if s.isCrashed {
		log.Printf("Server[%v]: crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.term += 1
	s.isLeaderMutex.Unlock()

	for i := range s.peers {
		if int64(i) == s.ID {
			continue
		}
		s.matchIndex[i] = -1
		s.nextIndex[i] = int64(len(s.log)) // last log index + 1
	}

	return &Success{
		Flag: true,
	}, nil
}

// call AppendEntries() to all the peers
// check matchIndex, majority and current term, update commitIndex
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Printf("Server[%v]: SendHeartbeat\n", s.ID)
	s.isCrashedMutex.RLocker().Lock()
	if s.isCrashed {
		log.Printf("Server[%v]: crashed\n", s.ID)
		s.isCrashedMutex.RLocker().Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RLocker().Unlock()

	s.isLeaderMutex.RLocker().Lock()
	if !s.isLeader {
		log.Printf("Server[%v]: not a leader\n", s.ID)
		s.isLeaderMutex.RLocker().Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RLocker().Unlock()

peerLoop:
	for peerIndex, followerAddr := range s.peers {
		if int64(peerIndex) == s.ID {
			continue
		}
		log.Printf("Server[%d]: sendHearBeat to Server[%d]\n", s.ID, peerIndex)
		conn, err := grpc.Dial(followerAddr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		for {
			var prevLogTerm int64 = 0
			if s.nextIndex[peerIndex] != 0 {
				prevLogTerm = s.log[s.nextIndex[peerIndex]-1].Term
			}
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: s.nextIndex[peerIndex] - 1,
				PrevLogTerm:  int64(prevLogTerm),
				Entries:      s.log[s.nextIndex[peerIndex]:],
				LeaderCommit: s.commitIndex,
			}
			client := NewRaftSurfstoreClient(conn)
			output, err := client.AppendEntries(ctx, input)
			log.Printf("Server[%d]: follower[%d] return %v\n", s.ID, peerIndex, output)
			if err != nil { // follower crashed
				break
			}
			if output.Success { // follower commited
				log.Printf("Server[%v]: Follower[%d] commited, update matchIndex to %v and nextIndex to %v \n", s.ID, peerIndex, output.MatchedIndex, int64(len(s.log)))
				s.nextIndex[peerIndex] = int64(len(s.log))
				s.matchIndex[peerIndex] = output.MatchedIndex
				break
			} else if s.term < output.Term {
				log.Printf("Server[%d]: I am not the leader anymore\n", s.ID)
				s.term = output.Term
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				break peerLoop
			}
			if s.nextIndex[peerIndex] != 0 {
				s.nextIndex[peerIndex] -= 1
			}
		}

	}
	// update commitIndex, commit and apply
	for commitIndex := s.commitIndex + 1; commitIndex < int64(len(s.log)); commitIndex += 1 {
		if s.MajorityCommited(commitIndex) {
			s.commitIndex = commitIndex
		}
	}
	log.Printf("Server[%d]: after heartbeat, the commitIndex is %v\n", s.ID, s.commitIndex)
	for s.lastApplied < s.commitIndex {
		_, err := s.metaStore.UpdateFile(ctx, s.log[s.lastApplied+1].FileMetaData)
		if err != nil {
			log.Printf("Server[%d]: SendHeartBeat updatefile failed\n", s.ID)
			return nil, err
		}
		s.lastApplied += 1
	}
	log.Printf("Server[%d]: after heartbeat, the lastApplied is %v\n", s.ID, s.lastApplied)
	return &Success{
		Flag: true,
	}, nil
}

func (s *RaftSurfstore) MajorityCommited(commitIndex int64) bool {
	count := 1
	for peerIndex := range s.peers {
		if int64(peerIndex) == s.ID {
			continue
		}
		if s.matchIndex[peerIndex] >= commitIndex {
			count += 1
		}
	}
	return count > len(s.peers)/2 && s.log[commitIndex].Term == s.term
	// return count > len(s.peers)/2
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
