// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"strconv"

	"github.com/pingcap/log"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/schedule"
	"github.com/tikv/pd/server/schedulers"
	"go.uber.org/zap"
)

type slowStoreDetector struct {
	cluster          *RaftCluster
	evictedStoreId   uint64
	curSchedulerName string
}

func newSlowStoreDetector(cluster *RaftCluster) *slowStoreDetector {
	return &slowStoreDetector{
		cluster: cluster,
	}
}

func (d *slowStoreDetector) detectSlowNode(stores map[uint64]*core.StoreInfo) error {
	if d.evictedStoreId != 0 {
		evictedStore, ok := stores[d.evictedStoreId]
		if !ok || evictedStore.IsTombstone() {
			// Previous slow store had been removed, remove the sheduler and check
			// slow node next time.
			log.Info("delete slow store scheduler due to store has been removed",
				zap.Stringer("store", evictedStore.GetMeta()))
			return d.cluster.RemoveScheduler(d.curSchedulerName)
		}
		if !evictedStore.IsSlowRecovered() {
			log.Info("delete slow store scheduler due to store has been recovered",
				zap.Stringer("store", evictedStore.GetMeta()))
			return d.cluster.RemoveScheduler(d.curSchedulerName)
		}
	} else {
		slowStores := make([]*core.StoreInfo, 0)
		for _, store := range stores {
			if store.IsTombstone() {
				continue
			}

			if store.IsUp() && store.IsSlow() {
				slowStores = append(slowStores, store)
			}
		}

		if len(slowStores) == 1 {
			// TODO: add evict leader scheduler.
			store := slowStores[0]
			cfgDec := schedule.ConfigSliceDecoder(schedulers.EvictLeaderType, []string{strconv.FormatUint(store.GetID(), 10)})
			storage := d.cluster.GetStorage()
			opController := d.cluster.GetOperatorController()
			evictLeader, err := schedule.CreateScheduler(schedulers.EvictLeaderType, opController, storage, cfgDec)
			if err != nil {
				return err
			}
			err = d.cluster.AddScheduler(evictLeader)
			if err != nil {
				return err
			}
			d.evictedStoreId = store.GetID()
			d.curSchedulerName = evictLeader.GetName()
			log.Info("detected slow node, add evict leader scheduler",
				zap.String("scheduler-name", d.curSchedulerName),
				zap.Stringer("store", store.GetMeta()))
		} else if len(slowStores) > 1 {
			// TODO: alert to user here or another place.
		}
	}

	return nil
}
