import React, { useCallback, useState } from "react";
import { Alert, ButtonGroup } from "@mui/material";
import { Stack } from "@mui/system";
import SqlEditor from "@/components/query-runner/SqlEditor";
import { trim } from "lodash";
import { rqlite } from "@/rqlite/client";
import { QueryResults } from "@/types/rqlite";
import { AxiosResponse } from "axios";
import QueryResponseGrid from "@/components/query-runner/QueryResponsesGrid";
import { Resizable } from "re-resizable";
import { PlayCircle } from "@mui/icons-material";
import DBTreeView from "@/components/query-runner/DBTreeView";
import { LoadingButton } from "@mui/lab";

const QueryExecutorPage = () => {
  const [editorContent, setEditorContent] = useState("");
  const [results, setResults] = useState<({ q: string } & QueryResults)[]>([]);
  const [isQueryRunning, setIsQueryRunning] = useState(false);

  const removeResultByIndex = (index: number) => {
    const newResults = [...results];
    newResults.splice(index, 1);
    setResults(newResults);
  };

  const submitQuery = async (q: string) => {
    let res: AxiosResponse<QueryResults, any>;
    if (q.startsWith("select ") || q.startsWith("pragma ")) {
      res = await rqlite.read([q]);
    } else {
      res = await rqlite.write([q]);
    }
    return res;
  };

  const onSubmit = async (content: string) => {
    if (isQueryRunning) {
      return;
    }

    setIsQueryRunning(true);
    const queries = content
      .split(";")
      .map((q) => trim(q, "\n").toLowerCase())
      .filter((q) => q.length > 0);

    let responses: ({ q: string } & QueryResults)[] = [];
    for (const q of queries) {
      try {
        const res = await submitQuery(q);
        responses = [{ q, ...res.data }, ...responses];
      } catch(e: any) {
        responses = [{ q, results: [{ error: e.message || "unknown error occurs" }], time: 0 }, ...responses];
      }
    }
    setResults([...responses, ...results]);
    setIsQueryRunning(false);
  };

  return (
    <>
      <div
        style={{
          flexGrow: 1,
          overflow: "hidden",
          // height: "calc(100vh - 60px)",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <Resizable
          maxHeight="75vh"
          defaultSize={{
            width: "100%",
            height: "55vh",
          }}
        >
          <Stack direction="row" height="100%">
            <Resizable
              minWidth="20px"
              defaultSize={{ width: "200px", height: "100%" }}
            >
              <DBTreeView
                onLoadTableData={(tableName: string) => {
                  onSubmit(`SELECT * FROM ${tableName} LIMIT 500`)
                }}
              />
            </Resizable>

            <Stack justifyContent="center" paddingX="2px" width="100%">
              <Alert severity="info" sx={{ margin: "6px" }}>
                Multiple queries must be separated by semicolon.
              </Alert>
              <ButtonGroup sx={{ marginBottom: "4px", marginX: "6px" }}>
                <LoadingButton
                  disableRipple
                  startIcon={<PlayCircle />}
                  variant="contained"
                  onClick={() => onSubmit(editorContent)}
                  loading={isQueryRunning}
                >
                  RUN (Shift + Enter)
                </LoadingButton>
              </ButtonGroup>
              <SqlEditor
                onChange={setEditorContent}
                onSubmit={() => onSubmit(editorContent)}
              />
            </Stack>
          </Stack>
        </Resizable>
        <div style={{ overflow: "scroll", flexGrow: 1 }}>
          <QueryResponseGrid data={results} onRemoveTab={removeResultByIndex} />
        </div>
      </div>
    </>
  );
};

export default QueryExecutorPage;
