#!/usr/bin/env python3
from parsimonious.grammar import Grammar

GRAMMAR = Grammar(r'''
    init = ws ( show_users /
                show_regions /
                show_workspaces /
                show_workspace_groups /
                select /
                create_workspace_group /
                create_workspace ) ws end? ws
    ws = ~"(\s*(/\*.*\*/)*\s*)*"
    like = ~"like"i ws qs ws
    extended = ~"extended"i ws
    in = ~"in"i ws qs ws
    qs = ~"\"[^\"]*\"|'[^\']*'" ws
    ident = ~"(`[^`]`\.|\w+\.)?(`[^`]+`|\w+|\*)" ws
    name = ~"[A-Z]\w*" ws
    compound_name = ident alias? ws
    comma = "," ws
    end = ";" ws
    alias = ~"as"i ws ~"(`[^`]+`|\w+)" ws
    projections = compound_name alias? (comma compound_name alias?)* ws
    table = compound_name ws
    select = ~"select"i ws projections ~"from"i ws table ws
    in_region = ~"in"i ws ~"region" ws (~"id" ws)? qs ws
    with_password = ~"with"i ws ~"password" ws qs ws
    with_firewall_ranges = ~"with"i ws ~"firewall"i ws ~"ranges"i ws qs (comma qs)* ws
    expires_at = ~"expires"i ws ~"at" ws qs ws
    in_group = ~"in"i ws ~"group" ws (~"id" ws)? qs ws
    with_size = ~"with"i ws ~"size"i ws qs ws
    show_users = ~"show"i ws ~"users"i ws like? ws
    show_workspaces = ~"show"i ws ~"workspaces"i ws in_group ( like / extended )* ws
    show_workspace_groups = ~"show"i ws ~"workspace"i ws ~"groups"i ws ( like / extended )* ws
    show_regions = ~"show"i ws ~"regions"i ws like? ws
    create_workspace_group = ~"create"i ws ~"workspace"i ws ~"group" ws qs in_region ( with_password / expires_at / with_firewall_ranges )* ws
    create_workspace = ~"create"i ws ~"workspace"i ws qs in_group with_size ws
''')
