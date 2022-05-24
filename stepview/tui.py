from rich.table import Table
from textual import events
from textual.app import App
from textual.widgets import Footer
from textual.widgets import Header
from textual.widgets import ScrollView


class StepViewTUI(App):
    """StepView shows a table with stepfunction statemachine summaries."""

    def __init__(self, table: Table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        # self.aws_profiles = aws_profiles
        # self.period = period

    async def on_load(self, event: events.Load) -> None:
        """Bind keys with the app loads (but before entering application
        mode)"""
        await self.bind("q", "quit", "Quit")
        await self.bind("escape", "quit", "Quit")

    async def on_mount(self, event: events.Mount) -> None:
        """Create and dock the widgets."""

        body = ScrollView(gutter=1)

        await self.view.dock(Header(), edge="top")
        await self.view.dock(Footer(), edge="bottom")
        await self.view.dock(body, edge="right")

        async def get_stepfunction_data():
            await body.update(self.table)

        await self.call_later(get_stepfunction_data)
