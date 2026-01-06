/*
The following code implements a UDP listener on a specified port (514) and stores
all incoming messages to a DuckDB table (syslog_logs in syslog.db file) together
with the current timestamp and the IP address the messages were received from.
It utilizes DuckDB appender and a simple channel queue to allow 1000+ messages per second.
*/
package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

type logEntry struct {
	timestamp time.Time
	ip        string
	log       string
}

type SyslogReceiver struct {
	udpConn       *net.UDPConn
	listenAddress string
	logChan       chan logEntry
	processDone   chan struct{}
}

type DuckDBWriter struct {
	connector     *duckdb.Connector
	conn          driver.Conn
	appender      *duckdb.Appender
	tableName     string
	listenAddress string
	dbPath        string
}

func NewSyslogReceiver(listenAddress string) (*SyslogReceiver, error) {
	receiver := &SyslogReceiver{
		listenAddress: listenAddress,
	}

	// Create UDP listener
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddress)
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve UDP address: %w", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("Failed to listen on UDP: %w", err)
	}

	receiver.udpConn = udpConn

	// Create buffered channel for log entries
	receiver.logChan = make(chan logEntry, 1000)
	receiver.processDone = make(chan struct{})

	return receiver, nil
}

func NewDuckDBWriter(dbPath, tableName string) (*DuckDBWriter, error) {
	writer := &DuckDBWriter{
		dbPath:    dbPath,
		tableName: tableName,
	}

	// Create DuckDB connector
	connector, err := duckdb.NewConnector(dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}
	writer.connector = connector

	// Create connection directly from connector
	conn, err := connector.Connect(context.Background())
	if err != nil {
		connector.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	writer.conn = conn

	// Create table if it doesn't exist
	err = writer.createTable()
	if err != nil {
		conn.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Create appender directly from driver.Conn
	appender, err := duckdb.NewAppenderFromConn(conn, "", tableName)
	if err != nil {
		conn.Close()
		connector.Close()
		return nil, fmt.Errorf("failed to create appender: %w", err)
	}
	writer.appender = appender

	return writer, nil
}

func (writer *DuckDBWriter) createTable() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			timestamp TIMESTAMP,
			ip VARCHAR,
			log VARCHAR
		)
	`, writer.tableName)
	_, err := writer.conn.(driver.ExecerContext).ExecContext(context.Background(), query, nil)
	return err
}

func (s *SyslogReceiver) Start(writer *DuckDBWriter, ctx context.Context) error {
	log.Printf("Starting syslog receiver on %s", s.listenAddress)

	// Start goroutine to process log entries from buffer
	go s.processLogs(writer, ctx)

	buffer := make([]byte, 65507) // Max UDP packet size

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Set read deadline for context cancellation
			err := s.udpConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			if err != nil {
				return err
			}

			n, addr, err := s.udpConn.ReadFromUDP(buffer)
			if err != nil {
				// Check if error is due to timeout (for context cancellation)
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				log.Printf("Error reading UDP packet: %v", err)
				continue
			}

			// Extract IP address
			ip := addr.IP.String()

			// Extract log message
			logMsg := string(buffer[:n])

			// Get current timestamp
			timestamp := time.Now()

			// Send to channel (buffered, non-blocking if buffer not full)
			select {
			case s.logChan <- logEntry{timestamp: timestamp, ip: ip, log: logMsg}:
				// Successfully sent to channel
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Channel full, log warning but continue
				// Some clever throttling mechanism can be implemented instead
				log.Printf("Warning: log channel full, dropping message from %s", ip)
			}
		}
	}
}

func (receiver *SyslogReceiver) processLogs(writer *DuckDBWriter, ctx context.Context) {
	defer close(receiver.processDone)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	batchCount := 0
	const batchSize = 100

	for {
		select {
		case <-ctx.Done():
			// Process remaining entries in channel before exit
			for {
				select {
				case entry := <-receiver.logChan:
					err := writer.appender.AppendRow(entry.timestamp, entry.ip, entry.log)
					if err != nil {
						log.Printf("Error appending log to DuckDB: %v", err)
						continue
					}
					batchCount++
				default:
					// No more entries in channel
					if batchCount > 0 {
						writer.flushAppender()
					}
					return
				}
			}

		case entry, ok := <-receiver.logChan:
			if !ok {
				// Channel closed, flush and exit
				if batchCount > 0 {
					writer.flushAppender()
				}
				return
			}

			// Append log entry
			err := writer.appender.AppendRow(entry.timestamp, entry.ip, entry.log)
			if err != nil {
				log.Printf("Error appending log to DuckDB: %v", err)
				continue
			}

			batchCount++

			// Flush every 100 rows
			if batchCount >= batchSize {
				err = writer.flushAppender()
				if err != nil {
					log.Printf("Error flushing appender: %v", err)
				}
				batchCount = 0
			}

		case <-ticker.C:
			// Flush every 1 second if there are any pending entries
			if batchCount > 0 {
				err := writer.flushAppender()
				if err != nil {
					log.Printf("Error flushing appender: %v", err)
				}
				batchCount = 0
			}
		}
	}
}

func (writer *DuckDBWriter) flushAppender() error {
	err := writer.appender.Flush()
	if err != nil {
		return fmt.Errorf("Failed to flush appender: %w", err)
	}
	return nil
}

func (receiver *SyslogReceiver) Close() error {
	var errs []error

	if receiver.udpConn != nil {
		if err := receiver.udpConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("Failed to close UDP connection: %w", err))
		}
	}

	// Wait for processLogs goroutine to finish (if it exists)
	if receiver.processDone != nil {
		select {
		case <-receiver.processDone:
			// Goroutine finished
		case <-time.After(5 * time.Second):
			log.Printf("Warning: processLogs goroutine did not finish in time")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Errors during close: %v", errs)
	}

	return nil
}

func (writer *DuckDBWriter) Close() error {
	var errs []error

	if writer.appender != nil {
		if err := writer.appender.Close(); err != nil {
			errs = append(errs, fmt.Errorf("Failed to close appender: %w", err))
		}
	}

	if writer.conn != nil {
		if err := writer.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("Failed to close database connection: %w", err))
		}
	}

	if writer.connector != nil {
		if err := writer.connector.Close(); err != nil {
			errs = append(errs, fmt.Errorf("Failed to close connector: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Errors during close: %v", errs)
	}

	return nil
}

func main() {
	// Configuration - can be moved to environment variables or a config file
	listenAddress := ":514"    // Default syslog UDP port
	dbPath := "syslog.db"      // DuckDB database file path
	tableName := "syslog_logs" // Table name

	// Create Syslog receiver
	receiver, err := NewSyslogReceiver(listenAddress)
	if err != nil {
		log.Fatalf("Failed to create syslog receiver: %v", err)
	}
	defer receiver.Close()

	// Create DuckDB Writer
	writer, err := NewDuckDBWriter(dbPath, tableName)
	if err != nil {
		log.Fatalf("Failed to create DuckDB writer: %v", err)
	}
	defer writer.Close()

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start receiver in a goroutine
	go func() {
		if err := receiver.Start(writer, ctx); err != nil && err != context.Canceled {
			log.Fatalf("Receiver error: %v", err)
		}
	}()

	log.Printf("Syslog receiver started. Listening on %s, storing to %s in table %s", listenAddress, dbPath, tableName)
	log.Println("Press Ctrl+C to stop")

	// Wait for signal
	<-sigChan
	log.Println("Shutting down...")
	cancel()

	// Give receiver time to finish
	time.Sleep(1 * time.Second)
	log.Println("Shutdown complete")
}
