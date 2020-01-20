import itertools

import plotly.colors as plotly_colors
import plotly.graph_objects as go


def plottable_events(events):
    epoch = None
    for e in events:
        if epoch is None:
            epoch = e['timestamp']
        assert e['timestamp'] >= epoch
        details = ''
        if e['event'] == 'await results':
            details = ' ({})'.format(', '.join(sorted(e['jobs'])))
        if 'job' not in e:
            continue
        yield dict(
            task=e['job'],
            time=e['timestamp'] - epoch,
            dim=e['event'] in {'add', 'await results', 'await worker slot'},
            done=e['event'] == 'finish',
            label=e['event'] + (f' ({details})' if details else ''),
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
        return dict(
            name='',  # event['task'],
            legendgroup=event['task'],
            mode='markers',
            # ids=[...]
            x=[event['time']],
            y=[y],
            text=[event['label']],
            marker=dict(symbol='diamond-open', color=color),
        )

    def rect(a, b, y, color):
        """Draw a rectangle between the two given events.

        The label/metadata of the rectangle are taken from the first event.
        """
        assert a['task'] == b['task']
        x0, x1 = a['time'], b['time']
        y0, y1 = y - bar_width, y + bar_width
        return dict(
            name=a['task'],
            legendgroup=a['task'],
            opacity=0.1 if a['dim'] else 0.7,
            mode='none',
            # ids=[...]
            # 4 corners, clockwise from top-left
            x=[x0, x1, x1, x0],
            y=[y1, y1, y0, y0],
            text=[a['label']] * 4,
            fill='toself',  # fill area enclosed by above points
            fillcolor=color,
            hoverinfo='all',
        )

    last_event_by_task = {}  # map task name -> last event seen for that task
    for e in plottable_events(events):
        task = e['task']
        y = get_row(task)
        color = get_color(task)
        fig.add_trace(marker(e, y, color))
        if task in last_event_by_task:
            fig.add_trace(rect(last_event_by_task[task], e, y, color))
        last_event_by_task[task] = e

    return fig
