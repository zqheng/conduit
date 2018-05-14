import { Link } from 'react-router-dom';
import PropTypes from 'prop-types';
import React from 'react';
import './../../css/version.css';

class Version extends React.Component {
  renderVersionCheck() {
    const {latest, error, isLatest} = this.props;

    if (!latest) {
      return (<div>
        Version check failed
        {error ? `: ${this.props.error}` : ''}
      </div>);
    }

    if (isLatest) return "Conduit is up to date";

    return (
      <div>
        A new version ({latest}) is available<br />
        <Link
          to="https://versioncheck.conduit.io/update"
          className="button primary"
          target="_blank">
          Update Now
        </Link>
      </div>
    );
  }

  render() {
    return (
      <div className="version">
        Running Conduit {this.props.releaseVersion}<br />
        {this.renderVersionCheck()}
      </div>
    );
  }

}

Version.propTypes = {
  error: PropTypes.string.isRequired,
  isLatest: PropTypes.bool.isRequired,
  latest: PropTypes.bool.isRequired,
  releaseVersion: PropTypes.string.isRequired,
};

export default Version;
