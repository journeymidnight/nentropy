/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package helper

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/nentropy/log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

var Logger *log.Logger

// sysInfo returns useful system statistics.
func sysInfo() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	return map[string]string{
		"host.name":      host,
		"host.os":        runtime.GOOS,
		"host.arch":      runtime.GOARCH,
		"host.lang":      runtime.Version(),
		"host.cpus":      strconv.Itoa(runtime.NumCPU()),
		"mem.used":       humanize.Bytes(memstats.Alloc),
		"mem.total":      humanize.Bytes(memstats.Sys),
		"mem.heap.used":  humanize.Bytes(memstats.HeapAlloc),
		"mem.heap.total": humanize.Bytes(memstats.HeapSys),
	}
}

// stackInfo returns printable stack trace.
func stackInfo() string {
	// Convert stack-trace bytes to io.Reader.
	rawStack := bufio.NewReader(bytes.NewBuffer(debug.Stack()))
	// Skip stack trace lines until our real caller.
	for i := 0; i <= 4; i++ {
		rawStack.ReadLine()
	}

	// Read the rest of useful stack trace.
	stackBuf := new(bytes.Buffer)
	stackBuf.ReadFrom(rawStack)

	// Strip GOPATH of the build system and return.
	return strings.Replace(stackBuf.String(), "src/", "", -1)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func ErrorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	Printf(5, msg, data...)
	Println(5, "With error: ", err.Error())
	Println(5, "System Info: ", sysInfo())
}

// fatalIf wrapper function which takes error and prints error messages.
func FatalIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	Printf(5, msg, data...)
	Println(5, "With error: ", err.Error())
	Println(5, "System Info: ", sysInfo())
	Println(5, "Stack trace: ", stackInfo())
	os.Exit(1)
}

// Printf calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Printf.
func Printf(level int, format string, v ...interface{}) {
	if Logger != nil {
		Logger.Printf(level, fmt.Sprintf(format, v...))
	}
}

// Print calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Print.
func Print(level int, v ...interface{}) {
	Logger.Print(level, fmt.Sprint(v...))
}

// Println calls l.Output to print to the logger.
// Arguments are handled in the manner of fmt.Println.
func Println(level int, v ...interface{}) {
	Logger.Println(level, fmt.Sprintln(v...))
}

// Fatal is equivalent to l.Print() followed by a call to os.Exit(1).
func Fatal(v ...interface{}) {
	Logger.Fatal(0, fmt.Sprint(v...))
}

// Fatalf is equivalent to l.Printf() followed by a call to os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	Logger.Fatalf(0, fmt.Sprintf(format, v...))
}

// Fatalln is equivalent to l.Println() followed by a call to os.Exit(1).
func Fatalln(v ...interface{}) {
	Logger.Fatalln(0, fmt.Sprintln(v...))
}

// Panic is equivalent to l.Print() followed by a call to panic().
func Panic(level int, v ...interface{}) {
	Logger.Panic(level, fmt.Sprint(v...))
}

// Panicf is equivalent to l.Printf() followed by a call to panic().
func Panicf(level int, format string, v ...interface{}) {
	Logger.Panicf(level, fmt.Sprintf(format, v...))
}

// Panicln is equivalent to l.Println() followed by a call to panic().
func Panicln(level int, v ...interface{}) {
	Logger.Panicln(level, fmt.Sprintln(v...))
}

func init() {
	Logger = log.New(os.Stdout, "[nentropy]", log.LstdFlags, 5)
}
