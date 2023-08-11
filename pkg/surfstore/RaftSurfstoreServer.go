package surfstore

import (
	context "context"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	serverId int64
	ipList		 []string
	ip 		 string
	isLeader bool
	term     int64
	log      []*UpdateOperation
	commitIndex int64
	pendingReplicas []chan bool
	lastApplied int64
	nextIndex []int64
	
	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed{
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	} else {
		majorityAlive := false
		for{ // assuming that if majority nodes dont return it blocks and keeps waiting here
			if majorityAlive{
				break
			}
			totalAlive := 1
			for id, addr := range s.ipList{
				if int64(id) == s.serverId{
					continue
				}
				func (){
					conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					client := NewRaftSurfstoreClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel() // Since term is -1 we want to always return false `append` entry if server is not crashed
					entries := make([]*UpdateOperation, 0)
					_, err := client.AppendEntries(ctx, &AppendEntryInput{Term: s.term, PrevLogIndex: -1, PrevLogTerm: -1, Entries: entries, LeaderCommit: s.commitIndex})
					if err!= nil && strings.Contains(err.Error(), STR_SERVER_CRASHED){
						conn.Close()
						return
					}
					if err == nil{
						totalAlive = totalAlive + 1 
					}
				}()
				if totalAlive > len(s.ipList)/2{
					majorityAlive = true
					break
				}
			}	
		}
		return s.metaStore.GetFileInfoMap(ctx, empty)
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	if s.isCrashed{
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	} else {
		majorityAlive := false
		for{ // assuming that if majority nodes dont return it blocks and keeps waiting here
			if majorityAlive{
				break
			}
			totalAlive := 1
			for id, addr := range s.ipList{
				if int64(id) == s.serverId{
					continue
				}
				func (){
					// log.Println("entered internal function")
					conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					// log.Println("conn created")
					client := NewRaftSurfstoreClient(conn)
					// log.Println("client created")
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					// log.Println("ctc created")
					defer cancel() // TO DO: what log to send
					entries := make([]*UpdateOperation, 0)
					_, err := client.AppendEntries(ctx, &AppendEntryInput{Term: s.term, PrevLogIndex: -1, PrevLogTerm: -1, Entries: entries, LeaderCommit: s.commitIndex})
					// log.Println("cleared appendEntries")
					if err!=nil && strings.Contains(err.Error(), STR_SERVER_CRASHED){
						conn.Close()
						return
					}
					if err == nil{
						totalAlive = totalAlive + 1 
					}
					// log.Println("exited internal function")
				}()
				if totalAlive > len(s.ipList)/2{
					majorityAlive = true
					break
				}
			}	
		}
		return s.metaStore.GetBlockStoreAddr(ctx, empty)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	log.Printf("Server%v: Entered Update File\n", s.serverId)
	if s.isCrashed{
		return nil, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return nil, ERR_NOT_LEADER
	}
	opr := UpdateOperation{
        Term: s.term,
        FileMetaData: filemeta,
    }
    s.log = append(s.log, &opr)
	replicated := make(chan bool)
	s.pendingReplicas = append(s.pendingReplicas, replicated)
	go s.AttemptReplication()
    success := <- replicated// even heartbeat can send value here
	if success { // block until successful
		log.Printf("Server%v: Successful in UpdateFile\n", s.serverId)
		for s.lastApplied < s.commitIndex{ // no need to check for updateFile errors because leader would have commited only if there are no errors
			s.lastApplied++
			entry := s.log[s.lastApplied]
			if s.lastApplied == s.commitIndex {
				return s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			} else {
				s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			}
		}
    } 
	return nil, nil
}

func (s *RaftSurfstore) AttemptReplication(){ // returns true when replication equal to leaders log is done at majority of servers
	targetIdx := s.commitIndex+1
	log.Printf("Server%v: AttemptReplication with targetIdx=%v\n", s.serverId,targetIdx)
	// var targetIdx int64
	// var heartbeat bool
	// if int(s.commitIndex+1) > len(s.log)-1{
	// 	heartbeat = true
	// 	targetIdx = int64(len(s.log)-1)
	// } else {
	// 	heartbeat = false
	// 	targetIdx = s.commitIndex+1
	// }
	// log.Printf("Entered Attempt Replication with targetId %d\n", targetIdx)
	replicateChan := make(chan *AppendEntryOutput, len(s.ipList))
    for idx, _ := range s.ipList {
        if int64(idx) == s.serverId {
            continue
        }
        go s.Replicate(int64(idx), targetIdx, replicateChan)
    }
    replicateCount := 1
	nodesProbed := 1
	for{
		if s.isCrashed{
			return
		}
		if nodesProbed >= len(s.ipList){
			return
		}
		rep := <-replicateChan
		if rep!=nil && rep.Success{
			replicateCount++
			nodesProbed++
		} else { nodesProbed++ } 
		if replicateCount > len(s.ipList)/2 && s.log[targetIdx].Term == s.term{ 
			log.Printf("Server%v: Received Majority Replica with targetIdx=%v\n", s.serverId,targetIdx)
			s.pendingReplicas[targetIdx] <- true // no need to signal second time otherwise will block
			s.commitIndex = targetIdx // dont break so that we can reuse for heartbeat; updatefile is already signalled	
			break
		}
	}
	log.Printf("Server%v: Exiting Attempt Replication\n", s.serverId)
}
func (s *RaftSurfstore) Replicate(followerId, entryId int64, replicateChan chan *AppendEntryOutput) {
	for {
		if s.isCrashed{
			replicateChan <- &AppendEntryOutput{ServerId: -1, Term: s.term, Success: false, MatchedIndex: -1}
			return
		}
		log.Printf("Server%v: Entered Replicate with entryId %v and nextId %v\n", s.serverId, entryId,s.nextIndex[followerId])
		// log.Printf("Entered Replicate with nextId %d and followerId %d\n", s.nextIndex[followerId], followerId)
	    addr := s.ipList[followerId]
        conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)
		var input *AppendEntryInput
		emptyEntries := make([]*UpdateOperation,0)
		var endId int64
		if int(entryId) >= len(s.log){
			endId = int64(len(s.log))
		} else {
			endId = entryId+1
		}
		// if int(entryId) >= len(s.log){ // just send a heartbeat to trigger commit
		// 	input = &AppendEntryInput{
		// 		Term: s.term,
		// 		PrevLogTerm: -1,
		// 		PrevLogIndex: -1,
		// 		Entries: emptyEntries, // nothing to be sent
		// 		LeaderCommit: s.commitIndex,
		// 	}
		//} else{
			if s.nextIndex[followerId] == 0{ // either initial stage or reached here after decrementation
				if len(s.log) != 0{ // something has been updated to the server 
					input = &AppendEntryInput{ // can be treated as heartbeat without any entries; but for case when log is empty
						Term: s.term,
						PrevLogTerm: -1,
						PrevLogIndex: -1,
						Entries: s.log[s.nextIndex[followerId]:endId],
						LeaderCommit: s.commitIndex,
					}
				}else {
					input = &AppendEntryInput{ // can be treated as heartbeat without any entries; but for case when log is empty
						Term: s.term,
						PrevLogTerm: -1,
						PrevLogIndex: -1,
						Entries: emptyEntries,
						LeaderCommit: s.commitIndex,
					}
				}
			} else {
				if int(s.nextIndex[followerId]) == len(s.log){
					input = &AppendEntryInput{
						Term: s.term,
						PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
						PrevLogIndex: s.nextIndex[followerId]-1,
						Entries: emptyEntries, // nothing to be sent
						LeaderCommit: s.commitIndex,
					}	
				} else {
					if s.nextIndex[followerId] <= entryId{
						input = &AppendEntryInput{
							Term: s.term,
							PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
							PrevLogIndex: s.nextIndex[followerId]-1,
							Entries: s.log[s.nextIndex[followerId]:endId],
							LeaderCommit: s.commitIndex,
						}		
					} else {
						input = &AppendEntryInput{
							Term: s.term,
							PrevLogTerm: s.log[s.nextIndex[followerId]-1].Term,
							PrevLogIndex: s.nextIndex[followerId]-1,
							Entries: emptyEntries,
							LeaderCommit: s.commitIndex,
						}
					}
				}
			}	
	//	}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		log.Printf("Server%v: Sending input:\n", s.serverId)
		log.Println(input)
		output, err := client.AppendEntries(ctx, input)
		if (output != nil) && (output.Term > s.term){
			s.term = output.Term
			s.isLeader = false
		}
		if output != nil && output.Success {
			log.Printf("Server%v: Received success from AppendEntry\n", s.serverId)
			s.nextIndex[followerId] = output.MatchedIndex+1
			replicateChan <- output
			conn.Close()
			return
		}
		if output != nil && !output.Success {
			log.Printf("Server%v: Received failure from AppendEntry\n", s.serverId)
			s.nextIndex[followerId]--
			if s.nextIndex[followerId] < 0{ // I dont think we wlil ever enter here
				s.nextIndex[followerId] = 0 // this basically means i need to send him everything 
				replicateChan <- &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
				conn.Close()
				return // added to handle crashed nodes
			}
		} 
		if err!= nil && (strings.Contains(err.Error(), STR_SERVER_CRASHED)){
			replicateChan <- &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
			return // added to handle crashed nodes
		}
		if err != nil{
			replicateChan <- &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
			conn.Close()
			return
		}
		conn.Close()
		cancel()
    }
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Printf("Server%v: Entered AppendEntry\n", s.serverId)
	// log.Printf("Entered AppendEntries\n")
	output := &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: false, MatchedIndex: -1}
	falseResponse := false
	if s.isCrashed{
		// log.Println("AppendEntries: crashed server")
		log.Printf("Server%v: AppendEntry Returned ERR_SERVER_CRASHED \n", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}
	if (input.Term < s.term) { // TODO; revisit - is it safe to return -1
		log.Printf("Server%v: AppendEntry Returned Stale Entry\n", s.serverId)
		// log.Println("AppendEntries: stale entry")
		return output, nil
	} // matchedIndex to be implemented
	if input.Term > s.term{
		s.isLeader = false
		s.term = input.Term
	}
	if (input.PrevLogTerm == -1) && (len(input.Entries)==0){ 
		falseResponse = true 
	}// if prevLogTerm is -1 no need to make below check because entire log needs to be replaced
	if input.PrevLogTerm != -1{
		log.Printf("Server%v: AppendEntry Inconsistency\n", s.serverId)
		// log.Println("AppendEntries: prev term not matching")
		if !(((len(s.log)-1) >= int(input.PrevLogIndex)) && (s.log[input.PrevLogIndex].Term == input.PrevLogTerm)){
			falseResponse = true	
		}	
	}
	// log.Println("entered append operation")
	if !falseResponse{
		log.Printf("Server%v: AppendEntry Entry Appended\n", s.serverId)
		s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)
		var chans []chan bool
		for i:=0; i<len(input.Entries); i++{
			c := make(chan bool)
			chans = append(chans, c)
		}
		s.pendingReplicas = append(s.pendingReplicas[:input.PrevLogIndex+1], chans...)
		output.Success = true
		output.MatchedIndex = int64(len(s.log)-1)
	}
	// log.Println("exited append operation")
	if (input.LeaderCommit > s.commitIndex){
		log.Printf("Server%v: AppendEntry had s.commitIndex %v LeaderCommit %v and lastLogIndex %v\n", s.commitIndex, s.serverId, input.LeaderCommit, len(s.log)-1)
		lastLogIndex := len(s.log)-1
		if lastLogIndex == -1 {
			lastLogIndex = 0
		}
		temp := int64(math.Min(float64(input.LeaderCommit), float64(lastLogIndex)))
		if int(temp) >= len(s.log){ return output, nil}
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(lastLogIndex)))
		for s.lastApplied < s.commitIndex{ // no need to check for updateFile errors because leader would have commited only if there are no errors
			//fmt.Println("hi")
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
		log.Printf("Server%v: AppendEntry set commitIndex as %v\n", s.serverId, s.commitIndex)
	}
	// log.Printf("Exiting AppendEntries\n")
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	log.Printf("Server%v: Entered SetLeader\n",s.serverId)
	if s.isCrashed {
		log.Printf("Server%v: Exited SetLeader\n",s.serverId)
		// log.Printf("Exiting SetLeader\n")
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	} else {
		s.term++
		s.isLeader = true
		log.Printf("Server%v: New Leader having commitIndex %v and log %v\n",s.serverId, s.commitIndex, s.log)
		for i:=0; i<len(s.ipList); i++{
			s.nextIndex[i] = int64(len(s.log))
		}
		log.Printf("Server%v: Exited SetLeader\n",s.serverId)
		// log.Printf("Exiting SetLeader\n")
		return &Success{Flag: true}, nil
	}
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// log.Printf("Entering SendHeartBeat\n")
	log.Printf("Server%v: Entered SendHeartBeat\n",s.serverId)
	if s.isCrashed{
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.isLeader{
		return &Success{Flag: false}, nil
	}
	s.AttemptReplication()
	// log.Printf("Exiting SendHeartBeat\n")
	log.Printf("Server%v: Exited SendHeartBeat\n",s.serverId)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
