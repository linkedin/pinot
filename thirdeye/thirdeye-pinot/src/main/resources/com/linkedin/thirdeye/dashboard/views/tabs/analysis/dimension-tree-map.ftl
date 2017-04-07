<div class="analysis-card padding-all top-buffer">
	<div class="analysis-title bottom-buffer">Contribution Analysis {{!--  (for {{metricName}} with filters: {{#each heatmapFilters}}{{@key}}: {{this}}{{/each}})--}}</div>
	<div class="contribution-analysis">
		<div class="contribution-analysis__daterangepicker">
			<label class="label-medium-semibold">Comparing:</label>
			<div class="datepicker-range" id="heatmap-current-range">
				<span></span>
				<b class="caret"></b>
			</div>
		</div>
		<div class="contribution-analysis__daterangepicker">
			<label class="label-medium-semibold">To:</label>
			<div class="datepicker-range" id="heatmap-baseline-range">
				<span></span>
				<b class="caret"></b>
			</div>
		</div>
	</div>

	<div class="row top-buffer bottom-buffer">
		<div class="col-xs-12">
			<nav class="navbar navbar-transparent" role="navigation">
				<div class="collapse navbar-collapse tree-map__nav">
					<ul class="nav navbar-nav tree-map-tabs" id="dashboard-tabs">
						<li class="tree-map__tab active" id="percent_change">
							<a class="tree-map__link" href="#percent_change" data-toggle="tab">% Change</a>
						</li>
						<li class="tree-map__tab" id="change_in_contribution">
							<a class="tree-map__link" href="#change_in_contribution" data-toggle="tab">Change in contribution</a>
						</li>
						<li class="tree-map__tab" id="contribution_to_overall_change">
							<a class="tree-map__link" href="#contribution_to_overall_change" data-toggle="tab">Contribution to overall change</a>
						</li>
					</ul>
				</div>
			</nav>
		</div>
	</div>
	<div id="dimension-tree-map-graph-placeholder"></div>
</div>
