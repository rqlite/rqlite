import ReactCodeMirror from "@uiw/react-codemirror";
import { sql, SQLite } from "@codemirror/lang-sql"
import { keymap } from "@codemirror/view";
import { useConfig } from "hooks/useConfig";

interface ISqlEditorProp {
  onSubmit: () => void;
  onChange: (content: string) => void;
}

const SqlEditor = (prop: ISqlEditorProp) => {
  const { config, saveEditorContent } = useConfig();

  return (
    <ReactCodeMirror
      height="100%"
      value={config.editorContent} // this sets initial value only.
      style={{ height: "calc(100% - 100px)", overflow: "scroll" }}
      extensions={[
        sql({ dialect: SQLite }),
        keymap.of([
          {
            key: "Shift-Enter",
            run: () => {
              prop.onSubmit();
              return true;
            },
          },
        ]),
      ]}
      onChange={(content) => {
        saveEditorContent(content);
        prop.onChange(content)
      }}
    />
  );
}

export default SqlEditor;
