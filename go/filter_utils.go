/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import "math"

type filterJobConfig struct {
	lgU      int
	capacity float64

	lgMinT int
	lgMaxT int
	tppo   int

	lgMinBpU int
	lgMaxBpU int

	minNumHashes int
	maxNumHashes int
}

func (c filterJobConfig) numItemsInserted() uint64 {
	return uint64(math.Round(c.capacity * float64(uint64(1)<<c.lgU)))
}

func (c filterJobConfig) getNumTrials(numHashes int) int {
	return numTrialsOnRamp(float64(numHashes), c.lgMinT, c.lgMaxT, c.lgMinBpU, c.lgMaxBpU)
}

type filterSpeedJobConfig struct {
	lgMinU int
	lgMaxU int
	uppo   int

	lgMinT int
	lgMaxT int

	lgMinBpU int
	lgMaxBpU int

	numSketches int

	numBits   uint64
	numHashes uint16
}

func (c filterSpeedJobConfig) getNumTrials(curU uint64) int {
	return numTrialsOnRamp(float64(curU), c.lgMinT, c.lgMaxT, c.lgMinBpU, c.lgMaxBpU)
}

type filterSpaceJobConfig struct {
	targetFpp float64

	lgMinU int
	lgMaxU int
	uppo   int

	lgMinT int
	lgMaxT int
	tppo   int

	lgMinBpU int
	lgMaxBpU int

	numHashesDelta int

	seed uint64
}

func (c filterSpaceJobConfig) getNumTrials(curU uint64) int {
	return numTrialsOnRamp(float64(curU), c.lgMinT, c.lgMaxT, c.lgMinBpU, c.lgMaxBpU)
}

func numTrialsOnRamp(x float64, lgMinT, lgMaxT, lgMinBpU, lgMaxBpU int) int {
	minBpU := float64(uint64(1) << lgMinBpU)
	maxBpU := float64(uint64(1) << lgMaxBpU)
	maxT := 1 << lgMaxT
	minT := 1 << lgMinT

	if lgMinT == lgMaxT || x <= minBpU {
		return maxT
	}
	if x >= maxBpU {
		return minT
	}
	// Negative slope: trials decay as the work per trial grows.
	slope := float64(lgMaxT-lgMinT) / float64(lgMinBpU-lgMaxBpU)
	lgX := math.Log(x) / math.Ln2
	lgTrials := slope*(lgX-float64(lgMinBpU)) + float64(lgMaxT)
	return int(math.Pow(2.0, lgTrials))
}
