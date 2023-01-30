class State:
    """We define a state object which provides some utility functions for the
    individual states within the state machine.
    """

    def __init__(self, http_client, job):
        self._http_client = http_client
        self._job = job

    def on_event(self, event):
        """
        Handle events that are delegated to this State.
        """
        pass

    def __repr__(self):
        """
        Leverages the __str__ method to describe the State.
        """
        return self.__str__()

    def __str__(self):
        """
        Returns the name of the State.
        """
        return self.__class__.__name__

    def on_event(event):
        pass


class StateMachine:
    """A simple state machine that mimics the functionality of a device from a
    high level.
    """

    def __init__(self, initial_state: State):
        """Initialize the components."""
        self.state = initial_state

    async def on_event(self, event):
        """
        This is the bread and butter of the state machine. Incoming events are
        delegated to the given states which then handle the event. The result is
        then assigned as the new state.
        """

        # The next state will be the result of the on_event function.
        self.state = await self.state.on_event(event)
