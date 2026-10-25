"use client";

import { useState } from "react";

import type { SearchDataMode } from "@/lib/search/backend-data";

import { IdentityProvider, useIdentity } from "../_components/identity-provider";
import { AdministratorSessionScreen } from "./administrator-session-screen";
import { AuthCardHeader, AuthFrame, AuthLoading } from "./auth-frame";
import { useLocationQuery } from "./auth-hooks";
import { prefillEmail } from "./email-address";
import { IdentitySignIn } from "./identity-sign-in";
import { safeReturnTo } from "./return-to";
import { useAuthNavigate } from "./use-auth-navigate";

interface SignInScreenProps {
  dataMode: SearchDataMode;
}

/**
 * `/signin/`. Account sign-in when identity is available; on a connected
 * server whose identity routes are not configured yet, the administrator
 * bearer session exactly as before.
 */
export function SignInScreen({ dataMode }: SignInScreenProps) {
  return (
    <IdentityProvider dataMode={dataMode}>
      <SignInRoute dataMode={dataMode} />
    </IdentityProvider>
  );
}

function SignInRoute({ dataMode }: SignInScreenProps) {
  const identity = useIdentity();
  const navigate = useAuthNavigate();
  const query = useLocationQuery();
  const [administratorToken, setAdministratorToken] = useState(false);
  const notConfigured = identity.status === "unavailable" && identity.error?.kind === "not_configured";
  const backend = dataMode === "backend";

  if (backend && (notConfigured || administratorToken)) {
    return <AdministratorSessionScreen onUseAccount={notConfigured ? undefined : () => setAdministratorToken(false)} />;
  }

  return (
    <AuthFrame footerNote={backend ? "Signed-in session" : "Preview directory · sample accounts"}>
      {query === null ? (
        <>
          <AuthCardHeader title="Sign in" />
          <AuthLoading label="Preparing sign-in" />
        </>
      ) : (
        <IdentitySignIn
          demo={!backend}
          initialEmail={prefillEmail(query.get("email"))}
          navigate={navigate}
          onUseAdministratorToken={backend ? () => setAdministratorToken(true) : undefined}
          returnTo={safeReturnTo(query.get("returnTo"))}
        />
      )}
    </AuthFrame>
  );
}
