class PlatformError(Exception):
    """Base for all Platform API errors."""


class PrivilegeDroppedError(PlatformError):
    """Raised when an operation requires a privilege that has been dropped."""


class QuotaExceededError(PlatformError):
    """Raised when a service exceeds its resource quota."""


class UnknownBlockError(PlatformError):
    """Raised when a service requests a block type it is not permitted to acquire."""


class ProvisioningError(PlatformError):
    """Raised when the underlying provisioning backend fails."""


class ReadinessTimeoutError(PlatformError):
    """Raised when a provisioned resource fails to become ready in time."""
