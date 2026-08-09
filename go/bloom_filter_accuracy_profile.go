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
	"math"
	"strings"

	"github.com/apache/datasketches-go/filters"
)

type BloomFilterAccuracyProfile struct {
	config filterJobConfig

	sketch           filters.BloomFilter
	filterLengthBits uint64
	numItemsInserted uint64
	vIn              uint64
}

func MustNewBloomFilterAccuracyProfile(cfg filterJobConfig) *BloomFilterAccuracyProfile {
	return &BloomFilterAccuracyProfile{
		config:           cfg,
		numItemsInserted: cfg.numItemsInserted(),
		vIn:              1,
	}
}

func (p *BloomFilterAccuracyProfile) run() {
	fmt.Println(p.getHeader())

	numQueries := uint64(1) << (p.config.minNumHashes + 1)

	sb := &strings.Builder{}
	for nh := p.config.minNumHashes; nh <= p.config.maxNumHashes; nh++ {
		fpr := 0.0
		filterNumBits := uint64(0)

		numTrials := p.config.getNumTrials(nh)
		for t := 0; t < numTrials; t++ {
			fpr += p.doTrial(nh, numQueries)
			filterNumBits += p.getFilterLengthBits()
		}
		fpr /= float64(numTrials)
		filterNumBits /= uint64(numTrials)

		p.process(nh, fpr, filterNumBits, numQueries, numTrials, sb)
		fmt.Println(sb.String())

		numQueries = pwr2SeriesNext(p.config.tppo, uint64(1)<<(nh+1))
	}
}

func (p *BloomFilterAccuracyProfile) doTrial(numHashes int, numQueries uint64) float64 {
	p.filterLengthBits = uint64(float64(uint64(numHashes)*p.numItemsInserted) / math.Ln2)

	sketch, err := filters.NewBloomFilterBySize(p.filterLengthBits, uint16(numHashes))
	if err != nil {
		panic(err)
	}
	p.sketch = sketch

	for i := uint64(0); i < p.numItemsInserted; i++ {
		p.vIn++
		if err := p.sketch.UpdateUInt64(p.vIn); err != nil {
			panic(err)
		}
	}

	numFalsePositive := uint64(0)
	for i := uint64(0); i < numQueries; i++ {
		p.vIn++
		if p.sketch.QueryUInt64(p.vIn) {
			numFalsePositive++
		}
	}
	return float64(numFalsePositive) / float64(numQueries)
}

func (p *BloomFilterAccuracyProfile) getFilterLengthBits() uint64 {
	return p.sketch.Capacity()
}

func (p *BloomFilterAccuracyProfile) getBitsPerEntry(numHashes int) int {
	return int(float64(numHashes) / math.Ln2)
}

func (p *BloomFilterAccuracyProfile) getHeader() string {
	return strings.Join([]string{
		"numHashes",
		"FPR",
		"filterSizeBits",
		"numQueryPoints",
		"numTrials",
	}, "\t")
}

func (p *BloomFilterAccuracyProfile) process(numHashes int, falsePositiveRate float64,
	filterSizeBits, numQueryPoints uint64, numTrials int, sb *strings.Builder) {
	sb.Reset()
	sb.WriteString(fmt.Sprintf("%d", numHashes))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%.5e", falsePositiveRate))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", filterSizeBits))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", numQueryPoints))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", numTrials))
}
