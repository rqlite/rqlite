 1055 // Test_Store_Reap_Full_FullWALs_NameCollision tests reaping when the name
    1056 // generated for the consolidated snapshot is identical to the name of the
    1057 // newest full snapshot. The resulting no-op rename must not leave behind a
    1058 // reap plan that prevents the store from reopening.
    1059 func Test_Store_Reap_Full_FullWALs_NameCollision(t *testing.T) {
    1060     dir := t.TempDir()
    1061     store, err := NewStore(dir)
    1062     if err != nil {
    1063         t.Fatalf("Failed to create new store: %v", err)
    1064     }
    1065     defer func() {
    1066         if store != nil {
    1067             store.Close()
    1068         }
    1069     }()
    1070 
    1071     // Install an older full snapshot so reaping has a snapshot to discard.
    1072     createSnapshotInStore(t, store, "2-1017-1704807719996", 1017, 2, 1, "testdata/db-and-wals/backup.db")
    1073 
    1074     // Use a fixed clock to ensure Store.Create and Reap generate the same name
    1075     // for the newest full snapshot.
    1076     store.snapshotNamer = NewSnapshotNamer(fixedClock(time.UnixMilli(2222222222222)))
    1077     sink, err := store.Create(1, 2000, 3, makeTestConfiguration("1", "localhost:1"), 1, nil)
    1078     if err != nil {
    1079         t.Fatalf("Failed to create snapshot sink: %v", err)
    1080     }
    1081     collidingID := sink.ID()
    1082 
    1083     streamer, err := NewSnapshotStreamer("testdata/db-and-wals/full2.db", "testdata/db-and-wals/full2-wal-00")
    1084     if err != nil {
    1085         t.Fatalf("Failed to create snapshot streamer: %v", err)
    1086     }
    1087     if err := streamer.Open(); err != nil {
    1088         t.Fatalf("Failed to open snapshot streamer: %v", err)
    1089     }
    1090     if _, err := io.Copy(sink, streamer); err != nil {
    1091         t.Fatalf("Failed to copy snapshot data: %v", err)
    1092     }
    1093     if err := streamer.Close(); err != nil {
    1094         t.Fatalf("Failed to close snapshot streamer: %v", err)
    1095     }
    1096     if err := sink.Close(); err != nil {
    1097         t.Fatalf("Failed to close snapshot sink: %v", err)
    1098     }
    1099 
    1100     // Keep the reap error long enough to verify that the persisted plan does not
    1101     // also prevent the store from reopening.
    1102     _, _, reapErr := store.Reap()
    1103     if err := store.Close(); err != nil {
    1104         t.Fatalf("Failed to close store: %v", err)
    1105     }
    1106     store = nil
    1107 
    1108     store, err = NewStore(dir)
    1109     if err != nil {
    1110         t.Fatalf("Store failed to reopen after reap (reap error: %v): %v", reapErr, err)
    1111     }
    1114     }
    1115 
    1116     if fsutil.FileExists(filepath.Join(dir, reapPlanFile)) {
    1117         t.Fatal("Expected REAP_PLAN to be removed after reap")
    1118     }
    1119     snaps := mustListSnapshots(t, store)
    1120     if len(snaps) != 1 {
    1121         t.Fatalf("Expected 1 snapshot after reap, got %d", len(snaps))
    1122     }
    1123     if snaps[0].ID != collidingID {
    1124         t.Fatalf("Expected remaining snapshot ID %s, got %s", collidingID, snaps[0].ID)
    1125     }
    1126 
    1127     _, rc, err := store.Open(snaps[0].ID)
    1128     if err != nil {
    1129         t.Fatalf("Failed to open snapshot: %v", err)
    1130     }
    1131     buf := &bytes.Buffer{}
    1132     if _, err := io.Copy(buf, rc); err != nil {
    1133         t.Fatalf("Failed to read snapshot: %v", err)
    1134     }
    1135     if err := rc.Close(); err != nil {
    1136         t.Fatalf("Failed to close snapshot reader: %v", err)
    1137     }
    1138     dbPath, walPaths := persistStreamerData(t, buf)
    1139     if len(walPaths) != 0 {
    1140         t.Fatalf("Expected 0 WAL files, got %d", len(walPaths))
    1141     }
    1142     rows := mustQueryDB(t, dbPath, "SELECT COUNT(*) FROM bar")
    1143     if exp, got := `[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]`, rows; exp != got {
    1144         t.Fatalf("unexpected results for query exp: %s got: %s", exp, got)
    1145     }
    1146 }

