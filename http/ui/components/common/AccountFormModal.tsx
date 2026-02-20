import { Dialog, DialogTitle, Button, Stack, TextField } from "@mui/material";
import { isEmpty } from "lodash";
import { useEffect, useState } from "react";

export interface IAccountFormModalProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (account: { username: string; password: string }) => void;
}

const AccountFormModal = (props: IAccountFormModalProps) => {
  const { onClose, open, onSubmit } = props;
  const [form, setForm] = useState({ username: "", password: "" });
  useEffect(() => {
    setForm({ username: "", password: "" });
  }, [open]);

  return (
    <Dialog onClose={onClose} open={open}>
      <Stack sx={{ padding: "10px" }}>
        <DialogTitle >Enter a <a href="https://rqlite.io/docs/guides/security/#basic-auth" target="_blank" rel="noreferrer">rqlite account</a></DialogTitle>
        <TextField
          value={form.username}
          onChange={(e) => setForm({ ...form, username: e.target.value })}
          label="username"
          style={{ marginBottom: "10px" }}
        />
        <TextField
          value={form.password}
          onChange={(e) => setForm({ ...form, password: e.target.value })}
          label="password"
        />
        <Button
          sx={{ marginTop: "20px" }}
          onClick={() => {
            if (isEmpty(form.username) || isEmpty(form.password)) {
              return;
            }

            onSubmit(form);
            onClose();
          }}
        >
          SUBMIT
        </Button>
      </Stack>
    </Dialog>
  );
};

export default AccountFormModal;
