from typing import TypeVar, Generic, Any

T = TypeVar('T')

class RunContext(Generic[T]):
    """
    Context class for running agent tools with dependencies.
    
    This class provides a context for running agent tools, including access to
    dependencies and other contextual information.
    """
    
    def __init__(self, deps: T):
        """
        Initialize the RunContext with dependencies.
        
        Args:
            deps: The dependencies for the agent tools.
        """
        self.deps = deps
