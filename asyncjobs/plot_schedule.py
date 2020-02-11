import itertools

import plotly.colors as plotly_colors
import plotly.graph_objects as go


def plottable_events(events):
    epoch = None
    for e in events:
        if epoch is None:
            epoch = e['timestamp']
        assert e['timestamp'] >= epoch
        wait_for = []
        if e['event'] == 'await results':
            wait_for = sorted(e['jobs'])
        details = ', '.join(wait_for) if wait_for else ''
        if 'job' not in e:
            continue
        yield dict(
            event=e['event'],
            task=e['job'],
            time=e['timestamp'] - epoch,
            done=e['event'] == 'finish',
            label=e['event'] + (f' ({details})' if details else ''),
            wait_for=wait_for,
        )


def plot_schedule(
    title,
    events,
    colors=None,
    bar_width=0.45,
    showgrid_x=True,
    showgrid_y=True,
    **kwargs,
):
    fig = go.Figure(
        layout=dict(
            title=title,
            showlegend=False,
            hovermode='closest',
            xaxis=dict(
                title='Time [seconds]',
                autorange=True,
                showgrid=showgrid_x,
                zeroline=False,
            ),
            yaxis=dict(
                title='Jobs',
                showgrid=showgrid_y,
                ticktext=[],  # rows.keys()
                tickvals=[],  # rows.values()
                range=[-1, 1],  # use rows.values() to update
                autorange=False,
                zeroline=False,
            ),
            **kwargs,
        ),
    )

    # Events are grouped into rows by their 'task'. The tasks are ordered
    # vertically by the appearance of its first event. Rows are drawn top-down,
    # i.e. in the _negative_ y-direction.
    rows = {}  # map task name -> y-coordinate from 0 and down

    def get_row(task):
        if task not in rows:
            rows[task] = -len(rows)
            fig.update_layout(
                dict(
                    yaxis=dict(
                        ticktext=list(rows.keys()),
                        tickvals=list(rows.values()),
                        range=[min(rows.values()) - 1, max(rows.values()) + 1],
                    )
                )
            )
        return rows[task]

    if colors is None:
        colors = {}  # map task name -> color
    # colors not already in colors are fetched from the list of default colors:
    palette = itertools.cycle(plotly_colors.DEFAULT_PLOTLY_COLORS)

    def get_color(task):
        if task not in colors:
            colors[task] = next(palette)
        return colors[task]

    def marker(event, y, color):
        """Draw a marker for the given event."""
        symbol = {
            'add': 'star',
            'start': 'diamond-wide-open',
            'await results': 'diamond-tall-open',
            'awaited results': 'diamond-wide-open',
            'await worker slot': 'diamond-tall-open',
            'awaited worker slot': 'diamond-wide-open',
            'await worker thread': 'triangle-right',
            'awaited worker thread': 'triangle-left',
            'await worker proc': 'triangle-right',
            'awaited worker proc': 'triangle-left',
            'finish': 'x',
        }
        return dict(
            name='',  # event['task'],
            legendgroup=event['task'],
            mode='markers',
            # ids=[...]
            x=[event['time']],
            y=[y],
            text=[event['label']],
            marker=dict(
                symbol=symbol.get(event['event'], 'square'),
                size=10,
                color=color,
            ),
        )

    def rect(a, b, y, color):
        """Draw a rectangle between the two given events.

        The label/metadata of the rectangle are taken from the first event.
        """
        assert a['task'] == b['task']
        x0, x1 = a['time'], b['time']
        y0, y1 = y - bar_width, y + bar_width
        opacity = {
            'add': 0.2,
            'start': 0.6,
            'await results': 0.2,
            'awaited results': 0.6,
            'await worker slot': 0.2,
            'awaited worker slot': 0.6,
            'await worker thread': 0.8,
            'awaited worker thread': 0.6,
            'await worker proc': 0.8,
            'awaited worker proc': 0.6,
        }
        return dict(
            name=a['task'],
            legendgroup=a['task'],
            opacity=opacity.get(a['event'], 1),
            mode='none',
            # ids=[...]
            # 4 corners, clockwise from top-left
            x=[x0, x1, x1, x0],
            y=[y1, y1, y0, y0],
            fill='toself',  # fill area enclosed by above points
            fillcolor=color,
            hoverinfo='text',
            text=a['label'],
        )

    def line(a, ay, b, by, color):
        """Draw a line from event a at y == ay to event b at y == by."""
        return dict(
            name='',
            opacity=0.75,
            mode='lines',
            x=[b['time'], a['time']],
            y=[by, ay],
            text=f"{a['task']} -> {b['task']}",
            line=dict(color=color, width=2, shape='hv', dash='dot'),
        )

    last_event_by_task = {}  # map task name -> last event seen for that task
    for e in plottable_events(events):
        task = e['task']
        y = get_row(task)
        color = get_color(task)
        fig.add_trace(marker(e, y, color))
        if task in last_event_by_task:
            last = last_event_by_task[task]
            fig.add_trace(rect(last, e, y, color))
            for t in last['wait_for']:
                other = last_event_by_task[t]
                assert other['done']
                fig.add_trace(line(other, get_row(t), e, y, color))
        last_event_by_task[task] = e

    return fig
