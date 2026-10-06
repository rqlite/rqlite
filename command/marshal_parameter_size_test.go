package command

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/rqlite/rqlite/v10/command/proto"
	pb "google.golang.org/protobuf/proto"
)

func parameterSizeRequest(kind string, parameter *proto.Parameter) Requester {
	request := &proto.Request{Statements: []*proto.Statement{{
		Sql: "INSERT INTO payloads VALUES(?)", Parameters: []*proto.Parameter{parameter},
	}}}
	switch kind {
	case "execute":
		return &proto.ExecuteRequest{Request: request}
	case "query":
		return &proto.QueryRequest{Request: request}
	default:
		return &proto.ExecuteQueryRequest{Request: request}
	}
}

func assertParameterSizeRoundTrip(t *testing.T, request Requester, wantCompressed bool) {
	t.Helper()
	data, compressed, err := NewRequestMarshaler().Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if compressed != wantCompressed {
		t.Errorf("compressed=%v, want %v (serialized request %d bytes)", compressed, wantCompressed, pb.Size(request))
	}
	recovered := request.ProtoReflect().Type().New().Interface()
	if err := UnmarshalSubCommand(&proto.Command{SubCommand: data, Compressed: compressed}, recovered); err != nil {
		t.Fatal(err)
	}
	if !pb.Equal(request, recovered) {
		t.Fatal("SQL, parameters or request options changed during round trip")
	}
}

func Test_MarshalParameterPayload(t *testing.T) {
	for _, kind := range []string{"execute", "query", "mixed"} {
		for _, blob := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/text", true: "/blob"}[blob], func(t *testing.T) {
				parameter := &proto.Parameter{Value: &proto.Parameter_S{S: strings.Repeat("P", 262144)}}
				if blob {
					parameter.Value = &proto.Parameter_Y{Y: make([]byte, 262144)}
				}
				assertParameterSizeRoundTrip(t, parameterSizeRequest(kind, parameter), true)
			})
		}
	}
}

func Test_MarshalSmallParameterStaysUncompressed(t *testing.T) {
	assertParameterSizeRoundTrip(t, parameterSizeRequest("execute", &proto.Parameter{
		Value: &proto.Parameter_S{S: "small"},
	}), false)
}

func Test_MarshalIncompressibleParameterKeepsOriginal(t *testing.T) {
	data := make([]byte, 262144)
	_, _ = rand.New(rand.NewSource(71)).Read(data)
	assertParameterSizeRoundTrip(t, parameterSizeRequest("execute", &proto.Parameter{
		Value: &proto.Parameter_Y{Y: data},
	}), false)
}

func Test_MarshalAggregateParameterSize(t *testing.T) {
	request := parameterSizeRequest("execute", &proto.Parameter{Value: &proto.Parameter_S{S: strings.Repeat("P", 512)}})
	for range 15 {
		request.GetRequest().Statements = append(request.GetRequest().Statements, request.GetRequest().Statements[0])
	}
	assertParameterSizeRoundTrip(t, request, true)
}

func Test_MarshalSerializedSizeBoundary(t *testing.T) {
	for _, size := range []int{defaultSizeThreshold - 1, defaultSizeThreshold} {
		request := parameterSizeRequest("execute", &proto.Parameter{Value: &proto.Parameter_S{S: ""}})
		for pb.Size(request) < size {
			value := request.GetRequest().Statements[0].Parameters[0].GetS()
			request.GetRequest().Statements[0].Parameters[0].Value = &proto.Parameter_S{S: value + "P"}
		}
		if pb.Size(request) != size {
			t.Fatalf("fixture does not reach exact serialized boundary %d", size)
		}
		assertParameterSizeRoundTrip(t, request, size >= defaultSizeThreshold)
	}
}

func Benchmark_ParameterPayload(b *testing.B) {
	random := make([]byte, 262144)
	_, _ = rand.New(rand.NewSource(71)).Read(random)
	for _, sample := range []struct {
		name      string
		parameter *proto.Parameter
	}{
		{"small", &proto.Parameter{Value: &proto.Parameter_S{S: "small"}}},
		{"compressible_256KiB", &proto.Parameter{Value: &proto.Parameter_S{S: strings.Repeat("P", 262144)}}},
		{"incompressible_256KiB", &proto.Parameter{Value: &proto.Parameter_Y{Y: random}}},
	} {
		b.Run(sample.name, func(b *testing.B) {
			request := parameterSizeRequest("execute", sample.parameter)
			marshaler := NewRequestMarshaler()
			b.SetBytes(int64(pb.Size(request)))
			b.ReportAllocs()
			var data []byte
			var err error
			for b.Loop() {
				data, _, err = marshaler.Marshal(request)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "wire-B/op")
		})
	}
}

func Test_SamplingHeuristicAndMisleadingData(t *testing.T) {
	random := make([]byte, 262144)
	_, _ = rand.New(rand.NewSource(71)).Read(random)
	if sampleCompressible(random) {
		t.Fatal("random sample should skip")
	}
	if !sampleCompressible(make([]byte, 262144)) {
		t.Fatal("zero sample should compress")
	}
	// A repeated high-entropy block is compressible, but the histogram misses it.
	repeated := make([]byte, 262144)
	for i := range repeated {
		repeated[i] = random[i%4096]
	}
	if sampleCompressible(repeated) {
		t.Fatal("fixture should show false negative")
	}
	assertParameterSizeRoundTrip(t, parameterSizeRequest("execute", &proto.Parameter{Value: &proto.Parameter_Y{Y: repeated}}), false)
	// Mostly random data can disguise itself with low-entropy sample windows.
	misleading := append([]byte(nil), random...)
	for i := 0; i < 4; i++ {
		start := i * (len(misleading) - 1024) / 3
		clear(misleading[start : start+1024])
	}
	if !sampleCompressible(misleading) {
		t.Fatal("fixture should show false positive")
	}
	request := parameterSizeRequest("execute", &proto.Parameter{Value: &proto.Parameter_Y{Y: misleading}})
	data, compressed, err := NewRequestMarshaler().Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	recovered := request.ProtoReflect().Type().New().Interface()
	if err := UnmarshalSubCommand(&proto.Command{SubCommand: data, Compressed: compressed}, recovered); err != nil || !pb.Equal(request, recovered) {
		t.Fatal("misleading sample changed data", err)
	}
	m := NewRequestMarshaler()
	m.ForceCompression = true
	_, compressed, err = m.Marshal(parameterSizeRequest("execute", &proto.Parameter{Value: &proto.Parameter_Y{Y: random}}))
	if err != nil || !compressed {
		t.Fatal("ForceCompression must bypass sampling", err)
	}
}
