from textual import events
from textual.app import App
from textual.widgets import Footer
from textual.widgets import Header
from textual.widgets import ScrollView

from stepview.data import main


class StepViewTUI(App):
    """StepView shows a table with stepfunction statemachine summaries."""

    # def __init__(self, aws_profiles, *args, **kwargs):
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.aws_profiles = ["datajob"]

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
            # table = main(aws_profiles=self.aws_profiles)
            table = main(aws_profiles=["datajob"])
            await body.update(table)

        await self.call_later(get_stepfunction_data)
