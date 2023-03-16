import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  CssBaseline,
  Menu,
  Tabs,
  Tab,
  Stack,
  MenuItem,
  IconButton,
  Divider,
  ListItemIcon,
  ListItemText,
} from "@mui/material";
import { FC, PropsWithChildren, useState } from "react";
import { useRouter } from "next/router";
import { useConfig } from "hooks/useConfig";
import { ArrowDropDown, Clear, ControlPoint } from "@mui/icons-material";
import AccountFormModal from "./AccountFormModal";
import { isEmpty } from "lodash";

const Layout: FC<PropsWithChildren> = ({ children }) => {
  const router = useRouter();
  const { config, removeAccount, activateAccount, addAccount } = useConfig();
  const activeUser = config.accounts.find((u) => u.isActive);
  const [accountFormModalOpen, setAccountFormModalOpen] = useState(false);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);
  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };
  const handleClose = () => {
    setAnchorEl(null);
  };

  return (
    <Box sx={{ display: "flex", flexDirection: "column" }}>
      <CssBaseline />
      <AppBar
        position="fixed"
        sx={{
          backgroundColor: "#eee",
          width: "100%",
          transition: "all .25s",
        }}
      >
        <Toolbar variant="dense">
          <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            width="100%"
          >
            <Stack direction="row" alignItems="center">
              <Typography
                variant="h6"
                noWrap
                component="div"
                color="black"
                fontWeight="300"
                marginRight="60px"
              >
                rqman
              </Typography>
              <Tabs
                value={router.pathname}
                onChange={(e, value) => router.push(value)}
              >
                <Tab value={"/menu/query-runner"} label="Query runner"></Tab>
                <Tab
                  value={"/menu/cluster-status"}
                  label="Cluster status"
                ></Tab>
              </Tabs>
            </Stack>
            <Stack>
              <Stack direction="row" alignItems="center" gap={1}>
                {/* <Popover
                  id={id}
                  open={open}
                  anchorEl={anchorEl}
                  onClose={handleClose}
                  anchorOrigin={{
                    vertical: "bottom",
                    horizontal: "left",
                  }}
                >
                  <Typography sx={{ p: 2 }}>
                    The content of the Popover.
                  </Typography>
                </Popover> */}
                {/* <Info style={{ fontSize: "18px" }} /> */}
                <span
                  style={{
                    alignItems: "center",
                    display: "flex",
                    backgroundColor: "#ddd",
                    borderRadius: "8px",
                    padding: "4px 4px 4px 10px",
                    cursor: "pointer",
                    color: "#333",
                  }}
                  id="account-button"
                  className="p-2 rounded-xl hover:bg-gray-100 cursor-pointer"
                  aria-controls={open ? "account-menu" : undefined}
                  aria-haspopup="true"
                  aria-expanded={open ? "true" : undefined}
                  onClick={handleClick}
                >
                  {activeUser?.username || "No account selected"}
                  <ArrowDropDown />
                </span>
              </Stack>
              <Menu
                id="account-menu"
                aria-labelledby="account-button"
                anchorEl={anchorEl}
                open={open}
                onClose={handleClose}
              >
                {isEmpty(config.accounts) && (
                  <MenuItem>
                    <span style={{ color: "#999" }}>
                      No accounts found, please add one.
                    </span>
                  </MenuItem>
                )}
                {config.accounts.map((u, index) => (
                  <MenuItem
                    key={u.username}
                    onClick={() => {
                      handleClose();
                      activateAccount(index);
                    }}
                    sx={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "4px 12px",
                    }}
                  >
                    <span>{u.username}</span>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        if (!window.confirm("Are you sure?")) {
                          return;
                        }
                        removeAccount(index);
                        handleClose();
                      }}
                    >
                      <Clear sx={{ fontSize: "16px" }} />
                    </IconButton>
                  </MenuItem>
                ))}
                <Divider />
                <MenuItem
                  onClick={() => {
                    handleClose();
                    setAccountFormModalOpen(true);
                  }}
                >
                  <ListItemIcon>
                    <ControlPoint />
                  </ListItemIcon>
                  <ListItemText>Add account</ListItemText>
                </MenuItem>
              </Menu>
              <AccountFormModal
                onSubmit={addAccount}
                open={accountFormModalOpen}
                onClose={() => setAccountFormModalOpen(false)}
              />
            </Stack>
          </Stack>
        </Toolbar>
      </AppBar>
      <main
        style={{
          width: "100%",
          transition: "all .25s",
          paddingTop: "48px",
          height: "100vh",
          display: "flex",
          flexDirection: "column",
        }}
      >
        {children}
      </main>
    </Box>
  );
};

export default Layout;
