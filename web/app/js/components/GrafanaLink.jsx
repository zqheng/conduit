import PropTypes from 'prop-types';
import React from 'react';

const GrafanaLink = ({ConduitLink, displayName, name, namespace, resource}) => {
  let resourceVariableName = resource.toLowerCase().replace(" ", "_");
  let dashboardName = resource.toLowerCase().replace(" ", "-");

  return (
    <ConduitLink
      to={`/dashboard/db/conduit-${dashboardName}?var-namespace=${namespace}&var-${resourceVariableName}=${name}`}
      deployment={"grafana"}
      targetBlank={true}>
      {displayName || name}&nbsp;&nbsp;<i className="fa fa-external-link" />
    </ConduitLink>
  );
};

GrafanaLink.propTypes = {
  ConduitLink: PropTypes.func.isRequired,
  displayName: PropTypes.string,
  name: PropTypes.string.isRequired,
  namespace: PropTypes.string.isRequired,
  resource: PropTypes.string.isRequired,
};

export default GrafanaLink;
