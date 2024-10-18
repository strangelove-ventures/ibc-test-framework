package handlers

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
)

const defaultTailLines = 50

type LogStream struct {
	fName   string
	authKey string
	logger  *zap.Logger
}

func NewLogSteam(logger *zap.Logger, file string, authKey string) *LogStream {
	return &LogStream{
		fName:   file,
		authKey: authKey,
		logger:  logger,
	}
}

func (ls *LogStream) StreamLogs(w http.ResponseWriter, r *http.Request) {
	if err := VerifyAuthKey(ls.authKey, r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Set headers to keep the connection open for SSE (Server-Sent Events)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Flush ensures data is sent to the client immediately
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Open the log file
	file, err := os.Open(ls.fName)
	if err != nil {
		http.Error(w, "Unable to open log file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Seek to the end of the file to read only new log entries
	file.Seek(0, io.SeekEnd)

	// Read new lines from the log file
	reader := bufio.NewReader(file)

	for {
		select {
		// In case client closes the connection, break out of loop
		case <-r.Context().Done():
			return
		default:
			// Try to read a line
			line, err := reader.ReadString('\n')
			if err == nil {
				// Send the log line to the client
				fmt.Fprintf(w, "data: %s\n\n", line)
				flusher.Flush() // Send to client immediately
			} else {
				// If no new log is available, wait for a short period before retrying
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (ls *LogStream) TailLogs(w http.ResponseWriter, r *http.Request) {
	if err := VerifyAuthKey(ls.authKey, r); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var linesToTail uint64 = defaultTailLines
	tailInput := r.URL.Query().Get("lines")
	if tailInput != "" {
		tailLines, err := strconv.ParseUint(tailInput, 10, 64)
		if err != nil {
			http.Error(w, "Invalid lines input", http.StatusBadRequest)
			return
		}
		linesToTail = tailLines
	}

	logs := TailFile(ls.logger, ls.fName, linesToTail)
	for _, log := range logs {
		fmt.Fprintf(w, "%s\n", log)
	}
}

func TailFile(logger *zap.Logger, logFile string, lines uint64) []string {
	// read the last n lines of a file
	file, err := os.Open(logFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	totalLines, err := lineCounter(file)
	if err != nil {
		log.Fatal(err)
	}

	if lines > uint64(totalLines) {
		lines = uint64(totalLines)
	}

	file.Seek(0, io.SeekStart)
	reader := bufio.NewReader(file)

	var logs []string
	for i := 0; uint64(i) < totalLines-lines; i++ {
		_, _, err := reader.ReadLine()
		if err != nil {
			logger.Fatal("Error reading log file", zap.Error(err))
		}
	}

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		logs = append(logs, string(line))
	}

	return logs
}

func lineCounter(r io.Reader) (uint64, error) {
	buf := make([]byte, 32*1024)
	var count uint64 = 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += uint64(bytes.Count(buf[:c], lineSep))

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}
