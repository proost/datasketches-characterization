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
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/datasketches-go/filters"
)

type BloomFilterUpdateSpeedProfile struct {
	config filterSpeedJobConfig

	sketch filters.BloomFilter
	vIn    uint64
}

func MustNewBloomFilterUpdateSpeedProfile(cfg filterSpeedJobConfig) *BloomFilterUpdateSpeedProfile {
	sketch, err := filters.NewBloomFilterBySize(cfg.numBits, cfg.numHashes)
	if err != nil {
		panic(err)
	}

	return &BloomFilterUpdateSpeedProfile{
		config: cfg,
		sketch: sketch,
		vIn:    1,
	}
}

func (p *BloomFilterUpdateSpeedProfile) run() {
	debug.SetMemoryLimit(math.MaxInt64)

	fmt.Println(p.getHeader())

	maxU := uint64(1) << p.config.lgMaxU
	minU := uint64(1) << p.config.lgMinU

	sb := &strings.Builder{}

	limit := 0.9 * float64(maxU)
	lastU := uint64(0)
	for float64(lastU) < limit {
		nextU := minU
		if lastU != 0 {
			nextU = pwr2SeriesNext(p.config.uppo, lastU)
		}
		lastU = nextU

		trials := p.config.getNumTrials(nextU)

		runtime.GC()

		sumUpdateTimePerUNanoSec := 0.0
		for t := 0; t < trials; t++ {
			sumUpdateTimePerUNanoSec += p.doTrial(nextU)
		}
		meanUpdateTimePerUNanoSec := sumUpdateTimePerUNanoSec / float64(trials)

		p.process(meanUpdateTimePerUNanoSec, trials, nextU, sb)
		fmt.Println(sb.String())
	}
}

func (p *BloomFilterUpdateSpeedProfile) doTrial(uPerTrial uint64) float64 {
	if err := p.sketch.Reset(); err != nil {
		panic(err)
	}

	start := time.Now()
	for u := uPerTrial; u > 0; u-- {
		p.vIn++
		p.sketch.UpdateUInt64(p.vIn)
	}
	elapsed := time.Since(start)

	return float64(elapsed.Nanoseconds()) / float64(uPerTrial)
}

func (p *BloomFilterUpdateSpeedProfile) getHeader() string {
	cols := []string{"InU", "Trials", "nS/Set"}
	if p.config.numSketches > 1 {
		cols = append(cols, "nS/Sketch")
	}
	return strings.Join(cols, "\t")
}

func (p *BloomFilterUpdateSpeedProfile) process(meanUpdateTimePerSetNanoSec float64,
	trials int, uPerTrial uint64, sb *strings.Builder) {
	sb.Reset()
	sb.WriteString(fmt.Sprintf("%d", uPerTrial))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", trials))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", meanUpdateTimePerSetNanoSec))
	if p.config.numSketches > 1 {
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%e", meanUpdateTimePerSetNanoSec/float64(p.config.numSketches)))
	}
}
