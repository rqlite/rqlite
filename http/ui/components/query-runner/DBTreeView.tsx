import { rqlite } from "@/rqlite/client";
import { ChevronRight, ExpandMore, RefreshOutlined } from "@mui/icons-material";
import { TreeItem, TreeView } from "@mui/lab";
import { flatten, get, isEmpty } from "lodash";
import { useQueries, useQuery } from "@tanstack/react-query";
import { useEffect } from "react";
import { Box } from "@mui/system";
import { IconButton } from "@mui/material";

interface IDBTreeViewProp {
  onLoadTableData: (table: string) => void;
}

const DBTreeView = (prop: IDBTreeViewProp) => {
  const query = "SELECT name FROM sqlite_master WHERE type='table'";
  const { data: tableResponse, isError, error, refetch: refetchTables } = useQuery(
    [query],
    () => rqlite.read([query]),
  );
  const tables = flatten(tableResponse?.data.results[0].values);
  
  const tableInfo = useQueries({
    queries: tables.map(t => {
      const q = `PRAGMA table_info(${t})`;
      return {
        queryKey: [q],
        queryFn: () => rqlite.read([q]),
        enabled: false,
      };
    }),
   });

  const refresh = async () => {
    refetchTables();
    tableInfo.forEach(i => {
      i.refetch();
    })
  };

  return (
    <div
      style={{ backgroundColor: "#eee", height: "100%", overflow: "scroll" }}
    >
      <Box sx={{ padding: "2px", display: "flex", alignItems: "center", justifyContent: "flex-end" }}>
        <IconButton onClick={refresh}>
          <RefreshOutlined />
        </IconButton>
      </Box>
      {isEmpty(tables) && (
        <div
          style={{
            padding: "10px",
            alignItems: "center",
            justifyContent: "center",
            display: "flex",
          }}
        >
          {isError
            ? get(error, "message", "Unknown error occurred while fetching data")
            : "No tables found"}
        </div>
      )}
      <TreeView
        defaultCollapseIcon={<ExpandMore />}
        defaultExpandIcon={<ChevronRight />}
      >
        {tables.map((table, index) => (
          <TreeItem
            nodeId={table}
            label={
              <span style={{ fontWeight: 600, fontSize: "15px" }}>{table}</span>
            }
            key={table}
            onClick={() => tableInfo[index].refetch()}
          >
            <TreeItem
              nodeId={table + "/data"}
              label="data"
              onClick={() => prop.onLoadTableData(table)}
            />
            <TreeItem nodeId={table + "/columns"} label="columns">
              {(tableInfo[index].data?.data.results[0].values || []).map(
                (column, index) => (
                  <TreeItem
                    nodeId={table + "/columns/" + index}
                    label={
                      <span style={{ fontSize: "14px" }}>
                        {column[1]}{" "}
                        <span style={{ color: "#777", fontSize: "12px" }}>
                          {column[2]}
                        </span>
                      </span>
                    }
                    key={index}
                  />
                )
              )}
            </TreeItem>
          </TreeItem>
        ))}
      </TreeView>
    </div>
  );
}

export default DBTreeView;
