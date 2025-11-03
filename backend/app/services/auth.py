"""
This service handles user authentication and authorization.
It provides functions to verify JSON Web Tokens (JWT) issued by Keycloak,
ensuring that API endpoints are protected and requests are made by authenticated users.
"""
import logging
import requests
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, jwk
from jose.exceptions import JWTError, ExpiredSignatureError, JWTClaimsError

from app.core.config import settings

# --- Setup logging ---
logger = logging.getLogger(__name__)
security = HTTPBearer()

# --- Fetch Keycloak public key ---
# This is done once at startup. In a production scenario, you might want
# a mechanism to periodically refresh these keys.
try:
    jwks_url = f"{settings.keycloak_issuer}/.well-known/openid-configuration"
    oidc_config = requests.get(jwks_url).json()
    jwks = requests.get(oidc_config["jwks_uri"]).json()
    PUBLIC_KEY = jwks
except requests.exceptions.RequestException as e:
    logger.error(f"Could not fetch Keycloak OIDC configuration: {e}")
    PUBLIC_KEY = None

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Validates the JWT token from the Authorization header against Keycloak.

    Args:
        credentials: The HTTP Authorization credentials (Bearer token).

    Raises:
        HTTPException: 401 if the token is invalid, expired, or has invalid claims.

    Returns:
        The decoded token payload (claims) as a dictionary.
    """
    if PUBLIC_KEY is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is not available (could not load keys)."
        )

    token = credentials.credentials
    try:
        # Get the unverified header to find the correct key
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}
        for key in PUBLIC_KEY["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]
                }
        
        if not rsa_key:
            raise HTTPException(status_code=401, detail="Invalid token header (kid not found).")

        # Decode and validate the token
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            audience=settings.keycloak_client_id,
            issuer=settings.keycloak_issuer
        )
        logger.info(f"Successfully validated token for user: {payload.get('email')}")
        return payload

    except ExpiredSignatureError:
        logger.warning("Token validation failed: Expired signature")
        raise HTTPException(status_code=401, detail="Token has expired.")
    except JWTClaimsError as e:
        logger.warning(f"Token validation failed: Invalid claims - {e}")
        raise HTTPException(status_code=401, detail=f"Invalid token claims: {e}")
    except JWTError as e:
        logger.warning(f"Token validation failed: {e}")
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during token validation: {e}")
        raise HTTPException(status_code=500, detail="Error validating token.")