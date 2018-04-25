import _ from 'lodash';
import CallToAction from './CallToAction.jsx';
import ConduitSpinner from "./ConduitSpinner.jsx";
import ErrorBanner from './ErrorBanner.jsx';
import { getPodCategorization } from './util/MetricUtils.js';
import { incompleteMeshMessage } from './util/CopyUtils.jsx';
import Metric from './Metric.jsx';
import PageHeader from './PageHeader.jsx';
import React from 'react';
import StatusTable from './StatusTable.jsx';
import { Col, Row, Table } from 'antd';
import './../../css/service-mesh.css';

const serviceMeshDetailsColumns = [
  {
    title: "Name",
    dataIndex: "name",
    key: "name"
  },
  {
    title: "Value",
    dataIndex: "value",
    key: "value",
    className: "numeric"
  }
];

export default class ServiceMesh extends React.Component {
  constructor(props) {
    super(props);
    this.loadFromServer = this.loadFromServer.bind(this);
    this.handleApiError = this.handleApiError.bind(this);
    this.api = this.props.api;

    this.state = {
      pollingInterval: 2000,
      metrics: [],
      deployments: [],
      components: [],
      lastUpdated: 0,
      pendingRequests: false,
      loaded: false,
      error: ''
    };
  }

  componentDidMount() {
    this.loadFromServer();
    this.timerId = window.setInterval(this.loadFromServer, this.state.pollingInterval);
  }

  componentWillUnmount() {
    window.clearInterval(this.timerId);
    this.api.cancelCurrentRequests();
  }

  loadFromServer() {
    if (this.state.pendingRequests) {
      return; // don't make more requests if the ones we sent haven't completed
    }
    this.setState({ pendingRequests: true });

    this.api.setCurrentRequests([
      this.api.fetchPods1("deploy"),
      this.api.fetchPods1("rc"),
    ]);

    this.serverPromise = Promise.all(this.api.getCurrentPromises())
      .then(([deploys, rcs]) => {
        let deployStatuses = _.get(deploys, ["ok", "podList", "rows"], []);
        let rcStatuses = _.get(rcs, ["ok", "podList", "rows"], []);
        let controlPlanePods = _.filter(deployStatuses, s => {
          return _.every(s.podStatuses, ps => ps.controlPlane);
        });

        this.setState({
          deployments: this.categorizePods(deployStatuses),
          replicationcontrollers: this.categorizePods(rcStatuses),
          components: controlPlanePods,
          lastUpdated: Date.now(),
          pendingRequests: false,
          loaded: true,
          error: ''
        });
      })
      .catch(this.handleApiError);
  }

  categorizePods(statuses) {
    return _.map(statuses, d => {
      _.each(d.podStatuses, pod => pod.value = getPodCategorization(pod));
      d.added = _.every(d.podStatuses, 'added');
      return d;
    });
  }

  handleApiError(e) {
    if (e.isCanceled) {
      return;
    }

    this.setState({
      pendingRequests: false,
      error: `Error getting data from server: ${e.message}`
    });
  }

  addedResourceCount(resource) {
    return _.size(_.filter(this.state[resource], ["added", true]));
  }

  unaddedResourceCount(resource) {
    return this.resourceCount(resource) - this.addedResourceCount(resource);
  }

  totalProxyCount() {
    return this.proxyCount("deployments") + this.proxyCount("replicationcontrollers");
  }

  proxyCount(resource) {
    return _.sum(_.map(_.concat(this.state[resource]), d => {
      return _.size(_.filter(d.podStatuses, ["value", "good"]));
    }));
  }

  componentCount() {
    return _.size(this.state.components);
  }

  resourceCount(resource) {
    return _.size(this.state[resource]);
  }

  getServiceMeshDetails() {
    return [
      { key: 1, name: "Conduit version", value: this.props.releaseVersion },
      { key: 2, name: "Conduit namespace", value: this.props.controllerNamespace },
      { key: 3, name: "Control plane components", value: this.componentCount() },
      { key: 4, name: "Added deployments", value: this.addedResourceCount("deployments") },
      { key: 5, name: "Unadded deployments", value: this.unaddedResourceCount("deployments") },
      { key: 6, name: "Added RCs", value: this.addedResourceCount("replicationcontrollers") },
      { key: 7, name: "Unadded RCs", value: this.unaddedResourceCount("replicationcontrollers") },
      { key: 8, name: "Data plane proxies", value: this.totalProxyCount() }
    ];
  }



  renderControlPlaneDetails() {
    return (
      <div className="mesh-section">
        <div className="clearfix header-with-metric">
          <div className="subsection-header">Control plane</div>
          <Metric title="Components" value={this.componentCount()} className="metric-large" />
        </div>

        <StatusTable
          componentColumnTitle="Component"
          data={this.state.components}
          statusColumnTitle="Pod Status"
          shouldLink={false}
          api={this.api} />
      </div>
    );
  }

  renderDataPlaneTable(resource, friendlyName, shortName) {
    return (
      <div className="mesh-section">
        <div className="clearfix header-with-metric">
          <div className="subsection-header">Data plane: {friendlyName}</div>
          <Metric title="Proxies" value={this.proxyCount(resource)} className="metric-large" />
          <Metric title={shortName || friendlyName} value={this.resourceCount(resource)} className="metric-large" />
        </div>

        <StatusTable
          data={this.state[resource]}
          componentColumnTitle={_.slice(friendlyName, 0, -1).join("")}
          statusColumnTitle="Proxy Status"
          shouldLink={true}
          api={this.api}  />
      </div>
    );
  }

  renderServiceMeshDetails() {
    return (
      <div className="mesh-section">
        <div className="clearfix header-with-metric">
          <div className="subsection-header">Service mesh details</div>
        </div>

        <div className="service-mesh-table">
          <Table
            className="conduit-table"
            dataSource={this.getServiceMeshDetails()}
            columns={serviceMeshDetailsColumns}
            pagination={false}
            size="middle" />
        </div>
      </div>
    );
  }

  renderAddResourceMessage(resource, friendlyName) {
    if (this.resourceCount(resource) === 0) {
      return (<div className="mesh-completion-message">
        No {friendlyName}s detected. {incompleteMeshMessage()}
      </div>);
    } else {
      let unaddedResources = this.unaddedResourceCount(resource);
      if (unaddedResources === 0) {
        return (<div className="mesh-completion-message">
        All {friendlyName}s have been added to the service mesh.</div>);
      } else {
        return (<div className="mesh-completion-message">
          {unaddedResources} {friendlyName}{unaddedResources === 1 ? "has" : "s have"} not been added to the mesh.
          {incompleteMeshMessage()}
        </div>);
      }
    }
  }

  renderControlPlane() {
    return (
      <Row gutter={16}>
        <Col span={16}>{this.renderControlPlaneDetails()}</Col>
        <Col span={8}>{this.renderServiceMeshDetails()}</Col>
      </Row>
    );
  }

  renderDataPlane() {
    return (
      <div>
        <Row gutter={16}>
          <Col span={16}>{this.renderDataPlaneTable("deployments", "Deployments")}</Col>
          <Col span={8}>{this.renderAddResourceMessage("deployments", "deployment")}</Col>
        </Row>
        <Row gutter={16}>
          <Col span={16}>{this.renderDataPlaneTable("replicationcontrollers", "Replication Controllers", "RCs")}</Col>
          <Col span={8}>{this.renderAddResourceMessage("replicationcontrollers", "replication controller")}</Col>
        </Row>
      </div>
    );
  }

  renderOverview() {
    if (this.proxyCount("deployments") === 0) {
      return <CallToAction numDeployments={this.resourceCount("deployments")} />;
    }
  }

  render() {
    return (
      <div className="page-content">
        { !this.state.error ? null : <ErrorBanner message={this.state.error} /> }
        { !this.state.loaded ? <ConduitSpinner /> :
          <div>
            <PageHeader
              header="Service mesh overview"
              hideButtons={this.proxyCount("deployments") === 0}
              api={this.api} />
            {this.renderOverview()}
            {this.renderControlPlane()}
            {this.renderDataPlane()}
          </div>
        }
      </div>
    );
  }
}
