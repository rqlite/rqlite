import Layout from '@/components/common/Layout'
import type { AppProps } from 'next/app'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createTheme, ThemeProvider } from '@mui/material';

export default function App({ Component, pageProps }: AppProps) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  const theme = createTheme({
    palette: {
      primary: {
        main: "#ffab00",
      },
      secondary: {
        main: "#f50057",
      },
    },
    typography: {
      button: {
        textTransform: "none",
      },
    },
    components: {
      MuiTabs: {
      },
      MuiTab: {
        styleOverrides: {
          root: {
            '&.Mui-selected': {
              color: 'black',
            },
          },
        }
      },
      MuiButtonBase: {
        defaultProps: {
          disableRipple: true,
        },
      },
    },
  });

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <Layout>
          <Component {...pageProps} />
        </Layout>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
