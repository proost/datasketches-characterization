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

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/datasketches-go/filters"
)

type BloomFilterSpaceProfile struct {
	config filterSpaceJobConfig

	vIn uint64
}

func MustNewBloomFilterSpaceProfile(cfg filterSpaceJobConfig) *BloomFilterSpaceProfile {
	return &BloomFilterSpaceProfile{config: cfg}
}

type spaceTrialResults struct {
	filterSizeBits uint64
	measuredFPR    float64
	numHashes      uint16
}

func (p *BloomFilterSpaceProfile) run() {
	fmt.Println(p.getHeader())

	maxU := uint64(1) << p.config.lgMaxU
	inputCardinality := pwr2SeriesNext(p.config.uppo, uint64(1)<<p.config.lgMinU)

	sb := &strings.Builder{}
	for inputCardinality < maxU {
		numTrials := p.config.getNumTrials(inputCardinality)

		inputCardinality = pwr2SeriesNext(p.config.uppo, inputCardinality)

		res := p.doTrial(inputCardinality)

		p.process(inputCardinality, res.filterSizeBits, numTrials, res.measuredFPR, res.numHashes, sb)
		fmt.Println(sb.String())
	}
}

func (p *BloomFilterSpaceProfile) doTrial(inputCardinality uint64) spaceTrialResults {
	numBits := filters.SuggestNumFilterBits(inputCardinality, p.config.targetFpp)
	suggestedNumHashes := filters.SuggestNumHashesFromSize(inputCardinality, numBits)

	numHashes := int(suggestedNumHashes) + p.config.numHashesDelta
	if numHashes < 1 {
		panic(fmt.Sprintf("numHashesDelta %d drives the hash count to %d at cardinality %d; "+
			"must stay >= 1", p.config.numHashesDelta, numHashes, inputCardinality))
	}

	sketch, err := filters.NewBloomFilterBySize(numBits, uint16(numHashes),
		filters.WithSeed(p.config.seed))
	if err != nil {
		panic(err)
	}

	numQueries := pwr2SeriesNext(p.config.tppo, uint64(1)<<suggestedNumHashes)

	for i := uint64(0); i < inputCardinality; i++ {
		p.vIn++
		if err := sketch.UpdateUInt64(p.vIn); err != nil {
			panic(err)
		}
	}

	numFalsePositive := uint64(0)
	for i := uint64(0); i < numQueries; i++ {
		p.vIn++
		if sketch.QueryUInt64(p.vIn) {
			numFalsePositive++
		}
	}

	return spaceTrialResults{
		filterSizeBits: sketch.Capacity(),
		measuredFPR:    float64(numFalsePositive) / float64(numQueries),
		numHashes:      sketch.NumHashes(),
	}
}

func (p *BloomFilterSpaceProfile) getHeader() string {
	return strings.Join([]string{
		"TrueU",
		"Size",
		"NumTrials",
		"FalsePositiveRate",
		"NumHashBits",
	}, "\t")
}

func (p *BloomFilterSpaceProfile) process(inputCardinality, sizeInBits uint64, numTrials int,
	falsePositiveRate float64, numHashes uint16, sb *strings.Builder) {
	sb.Reset()
	sb.WriteString(fmt.Sprintf("%d", inputCardinality))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", sizeInBits))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", numTrials))
	sb.WriteString("\t")
	sb.WriteString(strconv.FormatFloat(falsePositiveRate, 'g', -1, 64))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", numHashes))
}
