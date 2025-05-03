import { useRouter } from "next/router";
import { useEffect } from "react";

const NotFound = () => {
  const router = useRouter();
  useEffect(() => {
    router.replace("/menu/query-runner");
  }, [router])

  return null;
}

export default NotFound;
