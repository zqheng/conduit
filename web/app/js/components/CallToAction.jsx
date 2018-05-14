import _ from 'lodash';
import { incompleteMeshMessage } from './util/CopyUtils.jsx';
import PropTypes from 'prop-types';
import React from 'react';
import './../../css/cta.css';

const CallToAction = ({resource, numResources}) => (
  <div className="call-to-action">
    <div className="action summary">The service mesh was successfully installed!</div>

    <div className="action-steps">
      <div className="step-container complete">
        <div className="icon-container">
          <i className="fa fa-check-circle" aria-hidden="true" />
        </div>
        <div className="message"><p>Controller successfully installed</p></div>
      </div>

      <div className="step-container complete">
        <div className="icon-container">
          <i className="fa fa-check-circle" aria-hidden="true" />
        </div>
        <div className="message">{_.isNil(numResources) ? "No" : numResources} {resource}s detected</div>
      </div>

      <div className="step-container incomplete">
        <div className="icon-container">
          <i className="fa fa-circle-o" aria-hidden="true" />
        </div>
        <div className="message">Connect your first {resource}</div>
      </div>
    </div>

    <div className="clearfix">
      {incompleteMeshMessage()}
    </div>
  </div>
);

CallToAction.defaultProps = {
  resource: 'resource',
};

CallToAction.propTypes = {
  numResources: PropTypes.number,
  resource: PropTypes.string,
};

export default CallToAction;
