class PlatformError(Exception):
    """Base for all Platform API errors."""


class PrivilegeDroppedError(PlatformError):
    """Raised when acquire() is called after drop_to_scaling_only()."""


class InvalidStateError(PlatformError):
    """Raised when an operation is not permitted in the current state machine
    state but no privilege has actually been dropped (e.g. scale_hint()
    called before drop_to_scaling_only())."""


class QuotaExceededError(PlatformError):
    """Raised when a service exceeds its resource quota."""


class UnknownBlockError(PlatformError):
    """Raised when a service requests a block type it is not permitted to acquire."""


class ProvisioningError(PlatformError):
    """Raised when the underlying provisioning backend fails."""


class ReadinessTimeoutError(PlatformError):
    """Raised when a provisioned resource fails to become ready in time."""
