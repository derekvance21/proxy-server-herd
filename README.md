## Overview

This is an assignment from UCLA's CS130: Programming Languages course. It is a server herd application where an individual server can take client location updates, which it then propagates to connected servers, so that all servers have agreed upon client information. The client can then request nearby locations of any client which has sent location updates, which the server gets using the Google Places API. Every networked I/O operation is asynchronous, using Python's asyncio library.

The full write-up is at `report.pdf`.

