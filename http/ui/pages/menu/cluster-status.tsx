import { useQueries, UseQueryOptions } from "@tanstack/react-query";
import ReactCodeMirror from "@uiw/react-codemirror";
import { StreamLanguage } from "@codemirror/language";
import { yaml as yamlExtension } from "@codemirror/legacy-modes/mode/yaml";
import { Document as YAMLDocument } from "yaml";
import { Checkbox, FormControlLabel, FormGroup, Stack, Typography } from "@mui/material";
import { rqlite } from "@/rqlite/client";
import { capitalize } from "lodash";
import { useState } from "react";

type MonitoringType = "status" | "nodes" | "expvar";

const ClusterStatusPage = () => {
  const keys: MonitoringType[] = ["status", "nodes", "expvar"];
  const commonOption: UseQueryOptions = { refetchInterval: 3000 };
  const responses = useQueries({
    queries: [
      { queryKey: ["status"], queryFn: rqlite.status, ...commonOption },
      { queryKey: ["nodes"], queryFn: rqlite.nodes, ...commonOption },
      { queryKey: ["expvar"], queryFn: rqlite.expvar, ...commonOption },
      // TODO: add readiness status somewhere else
      // { queryKey: ["readyniess"], queryFn: rqlite.readiness },
    ],
  });
  const [tabVisibilities, setTabVisibilities] = useState<{
    status: boolean;
    nodes: boolean;
    expvar: boolean;
  }>({
    status: true,
    nodes: true,
    expvar: true,
  });

  return (
    <Stack height="calc(100% - 48px)">
      <FormGroup sx={{ paddingX: "10px" }}>
        <Stack direction="row">
          {keys.map((key) => {
            return (
              <FormControlLabel
                key={key}
                control={<Checkbox defaultChecked />}
                checked={tabVisibilities[key]}
                onChange={(e, value) => setTabVisibilities({ ...tabVisibilities, [key]: value })}
                label={capitalize(key)}
              />
            );
          })}
        </Stack>
      </FormGroup>
      <Stack direction="row" height="100%">
        {keys.map((key, index) => {
          if (!tabVisibilities[key]) {
            return null;
          }

          const doc = new YAMLDocument();
          if(responses[index].isError) {
            doc.contents = (responses[index].error as any)?.message || "Unknown error occurred while fetching data."
          } else {
            doc.contents = (responses[index].data as any)?.data;
          }
          
          return (
            <Stack direction="column" flex="1 1 0" width={0} key={key}>
              <Typography
                variant="h6"
                fontSize="18px"
                style={{
                  backgroundColor: "#eee",
                  padding: "6px",
                  borderLeft: "3px solid #ddd",
                }}
              >
                {capitalize(key)}
                {/* (API: /status, CLI: .status) */}
              </Typography>
              <ReactCodeMirror
                height="100%"
                style={{ width: "100%", height: "100%", overflow: "scroll" }}
                value={doc.toString()}
                readOnly
                // @ts-ignore
                extensions={[StreamLanguage.define(yamlExtension)]}
              />
            </Stack>
          );
        })}
      </Stack>
    </Stack>
  );
};

export default ClusterStatusPage;
